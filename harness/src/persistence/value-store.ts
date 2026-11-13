import { createHash, timingSafeEqual } from "node:crypto";
import { canonicalJson, copyJson, harnessError, parseJson } from "../contract/index.js";
import { type SQLiteStore, type ValuePublication } from "./sqlite.js";
import { type SQLWrite } from "./writes.js";

const CHUNK_BYTES = 65_536;
type ValueState = "staging" | "sealed" | "published" | "deleting";
export interface StoredValue extends ValuePublication { written: number; revision: number; state: ValueState }
export interface UnboundValueCandidate extends ValuePublication { written: number; revision: number; state: "staging" | "sealed" }

/**
 * Coordinator-private, durable byte staging. UTF-8 and SHA-256 integrity are
 * verified at seal; the caller must still validate the domain JSON/schema.
 * The logical quota reserves raw bytes, not SQLite/WAL/disk overhead.
 */
export class ValueStore {
  constructor(private readonly store: SQLiteStore, readonly quotaBytes: number) {
    if (!Number.isSafeInteger(quotaBytes) || quotaBytes <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid value quota");
  }

  /** Verify persisted logical accounting before accepting new uploads. This
   * single SELECT sees one SQLite snapshot, including interrupted deletion. */
  verifyReservation(): number {
    const row = this.store.query("SELECT reserved_bytes,COALESCE((SELECT SUM(total_bytes) FROM value_manifests),0) actual_bytes FROM value_capacity WHERE singleton=1")[0];
    if (!row || typeof row.reserved_bytes !== "number" || !Number.isSafeInteger(row.reserved_bytes) || row.reserved_bytes < 0
        || typeof row.actual_bytes !== "number" || !Number.isSafeInteger(row.actual_bytes) || row.actual_bytes !== row.reserved_bytes)
      throw harnessError("INTERNAL_ERROR", "Stored value quota accounting is inconsistent");
    return row.reserved_bytes;
  }

  /** Candidates only: absence of a binding never proves a writer has stopped. */
  unboundCandidates(after = "", limit = 64): UnboundValueCandidate[] {
    this.store.assertAuthority();
    if (typeof after !== "string" || !Number.isSafeInteger(limit) || limit < 1 || limit > 128)
      throw harnessError("INVALID_ARGUMENT", "Invalid unbound value scan");
    const rows = this.store.query(`SELECT m.id,m.owner,m.encoding,m.total_bytes,m.digest,m.written_bytes,m.revision,m.state,h.request_json
      FROM value_manifests m JOIN history h ON h.operation_id='value.begin:'||m.id AND h.type='value.begin' AND h.subject_id=m.id
      WHERE m.id>? AND m.state IN ('staging','sealed')
        AND NOT EXISTS (SELECT 1 FROM value_references r WHERE r.value_id=m.id)
        AND NOT EXISTS (SELECT 1 FROM report_transfer_bindings b WHERE b.control_value_id=m.id OR b.report_value_id=m.id)
      ORDER BY m.id LIMIT ?`, [after, limit]);
    return rows.map(row => {
      const value: ValuePublication = { id: row.id as string, owner: row.owner as string, encoding: row.encoding as string, length: row.total_bytes as number, digest: row.digest as string };
      this.checkInput(value);
      if (row.request_json !== canonicalJson(value) || !Number.isSafeInteger(row.written_bytes) || !Number.isSafeInteger(row.revision))
        throw harnessError("INTERNAL_ERROR", "Unbound value provenance is inconsistent");
      return { ...value, written: row.written_bytes as number, revision: row.revision as number, state: row.state as "staging" | "sealed" };
    });
  }

  /** Requires explicit source/admin authorization. A locked state transition
   * fences all future writers and races binding/publication in the same DB. */
  async retireUnbound(candidate: UnboundValueCandidate, guards: SQLWrite[]): Promise<void> {
    if (!candidate || !Array.isArray(guards) || !guards.length) throw harnessError("INVALID_ARGUMENT", "Authorized value retirement is required");
    candidate = copyJson(candidate) as unknown as UnboundValueCandidate;
    const { written, revision, state, ...value } = candidate;
    this.checkInput(value);
    if (!Number.isSafeInteger(written) || written < 0 || written > value.length || !Number.isSafeInteger(revision) || revision < 0 || !["staging", "sealed"].includes(state))
      throw harnessError("INVALID_ARGUMENT", "Invalid unbound value candidate");
    this.store.assertAuthority();
    const epoch = this.store.coordinatorEpoch, operationID = `value.retire-unbound:${value.id}`;
    const request = { value, written, revision, state };
    const prior = this.store.query("SELECT type,subject_id,request_json FROM history WHERE operation_id=?", [operationID])[0];
    if (prior) {
      if (prior.type !== "value.unbound_retired" || prior.subject_id !== value.id || prior.request_json !== canonicalJson(request))
        throw harnessError("REQUEST_CONFLICT", "Unbound retirement identity changed");
      this.store.checkGuards(guards);
    } else {
      const begin = this.store.query("SELECT type,subject_id,request_json FROM history WHERE operation_id=?", [`value.begin:${value.id}`])[0];
      if (!begin || begin.type !== "value.begin" || begin.subject_id !== value.id || begin.request_json !== canonicalJson(value))
        throw harnessError("STATE_CONFLICT", "Unbound value has no matching origin receipt");
      this.store.commit({ operationID, kind: "value.unbound_retired", subjectID: value.id, request: copyJson(request),
        absent: [{ table: "value_reference", id: value.id }], writes: [...guards,
          { sql: "UPDATE value_manifests SET state=?,revision=? WHERE id=? AND owner=? AND encoding=? AND total_bytes=? AND digest=? AND state=? AND revision=? AND written_bytes=?",
            params: ["deleting", revision + 1, value.id, value.owner, value.encoding, value.length, value.digest, state, revision, written], expectChanges: 1 }],
        evidence: null, result: null });
    }
    if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Unbound value retirement authority changed");
    await this.discard(value.id, value.owner);
  }

  /** Finish only durable, explicitly authorized unbound retirements. */
  async resumeUnboundRetirements(): Promise<number> {
    this.store.assertAuthority();
    const epoch = this.store.coordinatorEpoch; let cursor = "", completed = 0;
    for (;;) {
      const rows = this.store.query(`SELECT h.operation_id,h.subject_id,h.request_json FROM history h
        WHERE h.type='value.unbound_retired' AND h.operation_id>? AND EXISTS
          (SELECT 1 FROM value_manifests m WHERE m.id=json_extract(h.request_json,'$.value.id'))
        ORDER BY h.operation_id LIMIT 64`, [cursor]);
      if (!rows.length) return completed;
      for (const row of rows) {
        const request = parseJson(row.request_json as string) as unknown as { value: ValuePublication; written: number; revision: number; state: string };
        const value = request?.value;
        this.checkInput(value);
        if (row.subject_id !== value.id || row.operation_id !== `value.retire-unbound:${value.id}` || !Number.isSafeInteger(request.revision)
            || !Number.isSafeInteger(request.written) || !["staging", "sealed"].includes(request.state)) throw harnessError("INTERNAL_ERROR", "Unbound retirement receipt is invalid");
        const current = this.require(value.id, value.owner);
        if (current.state !== "deleting" || current.encoding !== value.encoding || current.length !== value.length || current.digest !== value.digest
            || current.revision !== request.revision + 1 || current.written !== request.written)
          throw harnessError("INTERNAL_ERROR", "Unbound retirement content changed");
        const begin = this.store.query("SELECT type,subject_id,request_json FROM history WHERE operation_id=?", [`value.begin:${value.id}`])[0];
        if (!begin || begin.type !== "value.begin" || begin.subject_id !== value.id || begin.request_json !== canonicalJson(value))
          throw harnessError("INTERNAL_ERROR", "Unbound retirement origin changed");
        if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Unbound value collection authority changed");
        await this.discard(value.id, value.owner); completed++; cursor = row.operation_id as string;
      }
      if (rows.length < 64) return completed;
    }
  }

  /** Additional source ownership is recorded atomically on first reservation;
   * replay preserves the original ownership rather than installing new writes. */
  begin(input: ValuePublication, originWrites: SQLWrite[] = []): StoredValue {
    this.checkInput(input);
    this.store.assertAuthority();
    const existing = this.find(input.id);
    if (existing) { this.checkSame(existing, input); return existing; }
    const reserved = this.store.query("SELECT reserved_bytes FROM value_capacity WHERE singleton=1")[0]!.reserved_bytes as number;
    if (!Number.isSafeInteger(reserved) || reserved < 0 || input.length > this.quotaBytes - reserved) throw harnessError("LIMIT_EXCEEDED", "Value staging quota is full");
    const writes: SQLWrite[] = [
      { sql: "UPDATE value_capacity SET reserved_bytes=? WHERE singleton=1 AND reserved_bytes=?", params: [reserved + input.length, reserved], expectChanges: 1 },
      { sql: "INSERT INTO value_manifests(id,owner,encoding,total_bytes,digest,written_bytes,revision,state,created_epoch) VALUES(?,?,?,?,?,?,?,?,?)",
        params: [input.id, input.owner, input.encoding, input.length, input.digest, 0, 0, "staging", this.store.coordinatorEpoch], expectChanges: 1 },
      ...originWrites,
    ];
    this.store.commit({ operationID: `value.begin:${input.id}`, kind: "value.begin", subjectID: input.id,
      request: { ...input }, writes, evidence: null, result: null });
    return this.require(input.id, input.owner);
  }

  append(id: string, owner: string, offset: number, bytes: Uint8Array, guards: SQLWrite[] = []): StoredValue {
    this.store.assertAuthority();
    if (!Number.isSafeInteger(offset) || offset < 0 || !(bytes instanceof Uint8Array) || bytes.byteLength === 0 || bytes.byteLength > CHUNK_BYTES)
      throw harnessError("INVALID_ARGUMENT", "Invalid value chunk");
    const copy = Buffer.from(bytes), value = this.require(id, owner);
    if (value.state === "deleting") throw harnessError("STATE_CONFLICT", "Value is being collected");
    if (!Array.isArray(guards)) throw harnessError("INVALID_ARGUMENT", "Invalid value chunk guards");
    if (offset < value.written) {
      this.checkChunk(id, offset, copy);
      if (guards.length) this.store.checkGuards(guards);
      this.checkChunk(id, offset, copy);
      return this.require(id, owner);
    }
    if (value.state !== "staging" || offset !== value.written || copy.length > value.length - offset)
      throw harnessError("INVALID_ARGUMENT", "Value chunks must be contiguous and within the reservation");
    const next = offset + copy.length, digest = createHash("sha256").update(copy).digest("hex");
    this.store.commit({ operationID: `value.append:${id}:${offset}`, kind: "value.append", subjectID: id,
      request: { id, owner, offset, length: copy.length, digest }, evidence: null, result: null,
      writes: [...guards,
        { sql: "UPDATE value_manifests SET written_bytes=?,revision=? WHERE id=? AND owner=? AND state=? AND revision=? AND written_bytes=?",
          params: [next, value.revision + 1, id, owner, "staging", value.revision, offset], expectChanges: 1 },
        { sql: "INSERT INTO value_chunks(value_id,offset_bytes,length_bytes,chunk_digest,payload_blob) VALUES(?,?,?,?,?)",
          params: [id, offset, copy.length, digest, { blob: copy.toString("base64") }], expectChanges: 1 },
      ] });
    // Even when an identical operation receipt exists, compare the stored
    // bytes, not just the chunk digest, before acknowledging this replay.
    if (guards.length) this.store.checkGuards(guards);
    this.checkChunk(id, offset, copy);
    return this.require(id, owner);
  }

  async seal(id: string, owner: string): Promise<ValuePublication> {
    this.store.assertAuthority();
    const epoch = this.store.coordinatorEpoch;
    const value = this.require(id, owner);
    if (value.state === "published" || value.state === "sealed") return this.publication(value);
    if (value.state !== "staging" || value.written !== value.length) throw harnessError("STATE_CONFLICT", "Value is not complete");
    const hash = createHash("sha256"), decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
    let offset = 0;
    while (offset < value.length) {
      const row = this.store.query("SELECT offset_bytes,length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [id, offset])[0];
      if (!row || !(row.payload_blob instanceof Uint8Array)) throw harnessError("INVALID_ARGUMENT", "Value chunk is missing");
      const data = Buffer.from(row.payload_blob);
      if (row.length_bytes !== data.length || !data.length || data.length > CHUNK_BYTES || data.length > value.length - offset
          || createHash("sha256").update(data).digest("hex") !== row.chunk_digest)
        throw harnessError("INVALID_ARGUMENT", "Value chunk length is invalid");
      try { decoder.decode(data, { stream: true }); } catch { throw harnessError("INVALID_ARGUMENT", "Value is not valid UTF-8"); }
      hash.update(data); offset += data.length;
      await new Promise<void>(resolve => setImmediate(resolve));
      this.store.assertAuthority();
      if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Value sealing authority changed");
    }
    try { decoder.decode(); } catch { throw harnessError("INVALID_ARGUMENT", "Value is not valid UTF-8"); }
    if (hash.digest("hex") !== value.digest) throw harnessError("INVALID_ARGUMENT", "Value digest does not match");
    this.store.commit({ operationID: `value.seal:${id}`, kind: "value.seal", subjectID: id,
      request: { id, owner, encoding: value.encoding, length: value.length, digest: value.digest }, evidence: null, result: null,
      writes: [{ sql: "UPDATE value_manifests SET state=?,revision=? WHERE id=? AND owner=? AND state=? AND revision=? AND written_bytes=?",
        params: ["sealed", value.revision + 1, id, owner, "staging", value.revision, value.length], expectChanges: 1 }] });
    return this.publication(this.require(id, owner));
  }

  /** Caller must reauthenticate/re-authorize before each use of yielded bytes. */
  async *read(id: string, owner: string): AsyncGenerator<Uint8Array> {
    yield* this.readParts(id, owner, false);
  }

  /** Private recovery proof only: an already published value can be read on
   * the freshly fenced recovery connection while normal authority is frozen. */
  async *readForEvidence(id: string, owner: string): AsyncGenerator<Uint8Array> {
    yield* this.readParts(id, owner, true);
  }

  private async *readParts(id: string, owner: string, evidence: boolean): AsyncGenerator<Uint8Array> {
    const assert = () => evidence ? this.store.assertEvidenceReadAuthority() : this.store.assertAuthority();
    assert();
    const epoch = this.store.coordinatorEpoch;
    const initial = this.require(id, owner);
    if (initial.state !== "published" && (evidence || initial.state !== "sealed")) throw harnessError("STATE_CONFLICT", "Value is unavailable");
    let offset = 0;
    while (offset < initial.length) {
      assert();
      if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Value reading authority changed");
      const current = this.require(id, owner);
      if (current.state !== "published" && (evidence || current.state !== "sealed") || current.length !== initial.length
          || current.digest !== initial.digest || current.encoding !== initial.encoding) throw harnessError("STATE_CONFLICT", "Value is unavailable");
      const row = this.store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [id, offset])[0];
      if (!row || !(row.payload_blob instanceof Uint8Array) || row.length_bytes !== row.payload_blob.byteLength || !row.payload_blob.byteLength
          || createHash("sha256").update(row.payload_blob).digest("hex") !== row.chunk_digest)
        throw harnessError("INTERNAL_ERROR", "Value chunk is unavailable");
      const data = Uint8Array.from(row.payload_blob); offset += data.byteLength;
      if (offset > initial.length) throw harnessError("INTERNAL_ERROR", "Value chunk exceeds its manifest");
      yield data;
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    assert();
    if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Value reading authority changed");
  }

  status(id: string, owner: string): StoredValue { this.store.assertAuthority(); return this.require(id, owner); }

  /** Bounded random access for a caller that revalidates its own authority for
   * each request. Only already-published content can be exposed this way. */
  readChunk(id: string, owner: string, offset: number, maxBytes: number, evidence = false): { bytes: Uint8Array; done: boolean } {
    if (evidence) this.store.assertEvidenceReadAuthority(); else this.store.assertAuthority();
    const value = this.require(id, owner);
    if (value.state !== "published") throw harnessError("STATE_CONFLICT", "Value is not published");
    if (!Number.isSafeInteger(offset) || offset < 0 || offset >= value.length || !Number.isSafeInteger(maxBytes) || maxBytes <= 0 || maxBytes > CHUNK_BYTES)
      throw harnessError("INVALID_ARGUMENT", "Invalid value read range");
    const row = this.store.query("SELECT offset_bytes,length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes<=? ORDER BY offset_bytes DESC LIMIT 1", [id, offset])[0];
    if (!row || !(row.payload_blob instanceof Uint8Array) || row.length_bytes !== row.payload_blob.byteLength || !row.payload_blob.byteLength
        || createHash("sha256").update(row.payload_blob).digest("hex") !== row.chunk_digest) throw harnessError("INTERNAL_ERROR", "Value chunk is unavailable");
    const begin = offset - (row.offset_bytes as number), available = row.payload_blob.byteLength - begin;
    if (begin < 0 || available <= 0) throw harnessError("INTERNAL_ERROR", "Value chunk range is unavailable");
    const bytes = Uint8Array.from(row.payload_blob.subarray(begin, begin + Math.min(available, maxBytes)));
    return { bytes, done: offset + bytes.length === value.length };
  }

  /** Collection is explicit; caller must establish that no live writer remains. */
  async discard(id: string, owner: string): Promise<void> {
    this.store.assertAuthority();
    const epoch = this.store.coordinatorEpoch;
    if (!this.find(id)) {
      const receipt = this.store.query("SELECT type,subject_id,request_json FROM history WHERE operation_id=?", [`value.remove:${id}`])[0];
      if (receipt?.type === "value.remove" && receipt.subject_id === id && receipt.request_json === canonicalJson({ id, owner })) return;
      throw harnessError("FORBIDDEN", "Value is unavailable");
    }
    let value = this.require(id, owner);
    if (value.state === "published") throw harnessError("STATE_CONFLICT", "Published value is retained");
    if (value.state !== "deleting") {
      this.store.commit({ operationID: `value.discard:${id}`, kind: "value.discard", subjectID: id,
        request: { id, owner }, absent: [{ table: "value_reference", id }], evidence: null, result: null,
        writes: [{ sql: "UPDATE value_manifests SET state=?,revision=? WHERE id=? AND owner=? AND state=? AND revision=?",
          params: ["deleting", value.revision + 1, id, owner, value.state, value.revision], expectChanges: 1 }] });
      value = this.require(id, owner);
    }
    for (;;) {
      const row = this.store.query("SELECT offset_bytes FROM value_chunks WHERE value_id=? ORDER BY offset_bytes LIMIT 1", [id])[0];
      if (!row) break;
      const offset = row.offset_bytes as number;
      this.store.commit({ operationID: `value.remove-chunk:${id}:${offset}`, kind: "value.remove_chunk", subjectID: id,
        request: { id, owner, offset }, evidence: null, result: null,
        writes: [{ sql: "DELETE FROM value_chunks WHERE value_id=? AND offset_bytes=?", params: [id, offset], expectChanges: 1 }] });
      await new Promise<void>(resolve => setImmediate(resolve));
      this.store.assertAuthority();
      if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Value collection authority changed");
    }
    const reserved = this.store.query("SELECT reserved_bytes FROM value_capacity WHERE singleton=1")[0]!.reserved_bytes as number;
    this.store.commit({ operationID: `value.remove:${id}`, kind: "value.remove", subjectID: id, request: { id, owner },
      absent: [{ table: "value_reference", id }], evidence: null, result: null,
      writes: [{ sql: "DELETE FROM value_manifests WHERE id=? AND owner=? AND state=?", params: [id, owner, "deleting"], expectChanges: 1 },
        { sql: "UPDATE value_capacity SET reserved_bytes=? WHERE singleton=1 AND reserved_bytes=?", params: [reserved - value.length, reserved], expectChanges: 1 }] });
  }

  private checkInput(input: ValuePublication): void {
    if (!input || Object.keys(input).sort().join(",") !== "digest,encoding,id,length,owner"
        || typeof input.id !== "string" || !input.id || input.id.trim() !== input.id
        || typeof input.owner !== "string" || !input.owner || input.owner.trim() !== input.owner
        || input.encoding !== "utf8-v1" || !Number.isSafeInteger(input.length) || input.length <= 0
        || typeof input.digest !== "string" || !/^[a-f0-9]{64}$/.test(input.digest)) throw harnessError("INVALID_ARGUMENT", "Invalid value manifest");
  }
  private find(id: string): StoredValue | null {
    const row = this.store.query("SELECT id,owner,encoding,total_bytes,digest,written_bytes,revision,state FROM value_manifests WHERE id=?", [id])[0];
    return row ? { id: row.id as string, owner: row.owner as string, encoding: row.encoding as string, length: row.total_bytes as number,
      digest: row.digest as string, written: row.written_bytes as number, revision: row.revision as number, state: row.state as ValueState } : null;
  }
  private require(id: string, owner: string): StoredValue {
    const value = this.find(id);
    if (!value || value.owner !== owner) throw harnessError("FORBIDDEN", "Value is unavailable");
    return value;
  }
  private checkSame(value: StoredValue, input: ValuePublication): void {
    if (value.owner !== input.owner || value.encoding !== input.encoding || value.length !== input.length || value.digest !== input.digest)
      throw harnessError("REQUEST_CONFLICT", "Value identity is bound to different content");
  }
  private checkChunk(id: string, offset: number, expected: Buffer): void {
    const row = this.store.query("SELECT payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [id, offset])[0];
    if (!row || !(row.payload_blob instanceof Uint8Array) || row.payload_blob.byteLength !== expected.length || !timingSafeEqual(row.payload_blob, expected))
      throw harnessError("REQUEST_CONFLICT", "Value chunk replay differs from stored bytes");
  }
  private publication(value: StoredValue): ValuePublication {
    return { id: value.id, owner: value.owner, encoding: value.encoding, length: value.length, digest: value.digest };
  }
}
