import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, harnessError, InternalCanonicalJsonDecoder, parseJson, validateNodeRequest, type ActionReport, type BusinessLeaseGrant, type SessionBinding } from "../contract/index.js";

export interface SpoolSource {
  nodeID: string; incarnation: string; grant: BusinessLeaseGrant; runID: string;
  actionID: string; attemptID: string; assignmentID: string; phase: "execute" | "reconcile";
}
export interface SpoolEntry { key: string; source: SpoolSource; report: ActionReport }
export type SpoolHead = SpoolEntry | { key: string; source: SpoolSource; chunked: true; length: number; digest: string };
export type SpoolHeadDescriptor = { kind: "inline"; head: SpoolHead } | { kind: "source-chunks"; key: string; sourceBytes: number;
  sourceDigest: string; sourceExpiresAt: number | null; contentBytes: number; digest: string };
const checksum = (source: string, report: string) => createHash("sha256").update(source, "utf8").update("\0").update(report, "utf8").digest("hex");

/** A logical byte budget for a node's durable, not-yet-acknowledged reports. */
export class ReportSpool {
  readonly #db: DatabaseSync;
  readonly #sourceComparisons = new Map<string, Iterator<Uint8Array>>();
  constructor(directory: string, readonly maxBytes: number, binding: SessionBinding, nodeID: string) {
    if (typeof directory !== "string" || !directory.trim() || !Number.isSafeInteger(maxBytes) || maxBytes <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid report spool configuration");
    mkdirSync(directory, { recursive: true });
    const db = this.#db = new DatabaseSync(join(directory, "report-spool.sqlite"));
    try {
      db.exec(`PRAGMA busy_timeout=1000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA locking_mode=EXCLUSIVE; PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS identity (domain_id TEXT NOT NULL, principal_id TEXT NOT NULL, node_id TEXT NOT NULL) STRICT;
        CREATE TABLE IF NOT EXISTS reports (key TEXT PRIMARY KEY, reserved INTEGER NOT NULL, source TEXT NOT NULL, report TEXT,
          state TEXT NOT NULL, content_bytes INTEGER, digest TEXT) STRICT;`);
      if (!db.prepare("PRAGMA table_info(identity)").all().some(column => column.name === "encoding")) db.exec("ALTER TABLE identity ADD COLUMN encoding TEXT NOT NULL DEFAULT 'inline'");
      const columns = db.prepare("PRAGMA table_info(reports)").all();
      if (!columns.some(column => column.name === "format")) db.exec("ALTER TABLE reports ADD COLUMN format TEXT NOT NULL DEFAULT 'inline'");
      if (!columns.some(column => column.name === "report_digest")) db.exec("ALTER TABLE reports ADD COLUMN report_digest TEXT");
      if (!columns.some(column => column.name === "source_format")) db.exec("ALTER TABLE reports ADD COLUMN source_format TEXT NOT NULL DEFAULT 'inline'");
      if (!columns.some(column => column.name === "source_bytes")) db.exec("ALTER TABLE reports ADD COLUMN source_bytes INTEGER");
      if (!columns.some(column => column.name === "source_digest")) db.exec("ALTER TABLE reports ADD COLUMN source_digest TEXT");
      if (!columns.some(column => column.name === "source_ready")) db.exec("ALTER TABLE reports ADD COLUMN source_ready INTEGER NOT NULL DEFAULT 1");
      if (!columns.some(column => column.name === "source_expires_at")) db.exec("ALTER TABLE reports ADD COLUMN source_expires_at INTEGER");
      db.exec(`CREATE TABLE IF NOT EXISTS report_chunks (
        report_key TEXT NOT NULL REFERENCES reports(key) ON DELETE CASCADE,
        offset_bytes INTEGER NOT NULL, bytes BLOB NOT NULL,
        PRIMARY KEY(report_key,offset_bytes)
      ) STRICT`);
      db.exec(`CREATE TABLE IF NOT EXISTS report_source_chunks (
        report_key TEXT NOT NULL REFERENCES reports(key) ON DELETE CASCADE,
        offset_bytes INTEGER NOT NULL, bytes BLOB NOT NULL,
        PRIMARY KEY(report_key,offset_bytes)
      ) STRICT`);
      db.exec(`CREATE TABLE IF NOT EXISTS identity_chunks (
        field TEXT NOT NULL, offset_bytes INTEGER NOT NULL, bytes BLOB NOT NULL,
        PRIMARY KEY(field,offset_bytes)
      ) STRICT`);
      const expected = { domain_id: binding.domainID, principal_id: binding.principalID, node_id: nodeID };
      if (Object.values(expected).some(value => typeof value !== "string" || !value)) throw harnessError("INVALID_ARGUMENT", "Invalid report spool identity");
      const expectedDigests: Record<string, string> = {};
      for (const [field, value] of Object.entries(expected)) {
        const hash = createHash("sha256"); for (const part of canonicalJsonChunks(value)) hash.update(part);
        expectedDigests[field] = hash.digest("hex");
      }
      db.exec("BEGIN IMMEDIATE");
      const identity = db.prepare("SELECT domain_id,principal_id,node_id,encoding FROM identity").all();
      if (identity.length > 1) throw harnessError("SESSION_MISMATCH", "Report spool belongs to another node or principal");
      if (!identity.length) {
        const long = Object.values(expected).some(value => value.length > 16_384 || Buffer.byteLength(value, "utf8") > 65_536);
        if (!long) db.prepare("INSERT INTO identity(domain_id,principal_id,node_id,encoding) VALUES(?,?,?,'inline')").run(binding.domainID, binding.principalID, nodeID);
        else db.prepare("INSERT INTO identity(domain_id,principal_id,node_id,encoding) VALUES(?,?,?,'staging-v1')")
          .run(expectedDigests.domain_id!, expectedDigests.principal_id!, expectedDigests.node_id!);
      }
      db.exec("COMMIT");
      const current = db.prepare("SELECT domain_id,principal_id,node_id,encoding FROM identity").get()!;
      if (current.encoding === "inline") {
        if (Object.entries(expected).some(([field, value]) => current[field] !== value)) throw harnessError("SESSION_MISMATCH", "Report spool belongs to another node or principal");
      } else if (current.encoding === "chunks-v1" || current.encoding === "staging-v1") {
        const staging = current.encoding === "staging-v1";
        if ((staging ? Object.entries(expectedDigests).some(([field, digest]) => current[field] !== digest)
              : current.domain_id !== "" || current.principal_id !== "" || current.node_id !== "")
            || db.prepare("SELECT 1 FROM identity_chunks WHERE field NOT IN ('domain_id','principal_id','node_id') LIMIT 1").get()
            || staging && db.prepare("SELECT 1 FROM reports LIMIT 1").get()) throw harnessError("SESSION_MISMATCH", "Report spool identity staging does not match");
        for (const [field, value] of Object.entries(expected)) {
          let offset = 0, count = 0;
          for (const part of canonicalJsonChunks(value)) {
            const piece = db.prepare("SELECT bytes FROM identity_chunks WHERE field=? AND offset_bytes=?").get(field, offset);
            if (piece) {
              if (!(piece.bytes instanceof Uint8Array) || !Buffer.from(piece.bytes).equals(Buffer.from(part)))
                throw harnessError("SESSION_MISMATCH", "Report spool belongs to another node or principal");
            } else {
              if (!staging || db.prepare("SELECT 1 FROM identity_chunks WHERE field=? AND offset_bytes>? LIMIT 1").get(field, offset))
                throw harnessError("SESSION_MISMATCH", "Report spool belongs to another node or principal");
              db.exec("BEGIN IMMEDIATE");
              try {
                if (db.prepare("SELECT encoding FROM identity").get()?.encoding !== "staging-v1") throw harnessError("STATE_CONFLICT", "Identity staging changed");
                db.prepare("INSERT INTO identity_chunks(field,offset_bytes,bytes) VALUES(?,?,?)").run(field, offset, part);
                db.exec("COMMIT");
              } catch (error) { db.exec("ROLLBACK"); throw error; }
            }
            offset += part.length; count++;
          }
          if (db.prepare("SELECT COUNT(*) n FROM identity_chunks WHERE field=?").get(field)?.n !== count)
            throw harnessError("SESSION_MISMATCH", "Report spool belongs to another node or principal");
        }
        if (staging) {
          db.exec("BEGIN IMMEDIATE");
          try {
            const changed = db.prepare("UPDATE identity SET domain_id='',principal_id='',node_id='',encoding='chunks-v1' WHERE encoding='staging-v1' AND domain_id=? AND principal_id=? AND node_id=? AND NOT EXISTS (SELECT 1 FROM reports)")
              .run(expectedDigests.domain_id!, expectedDigests.principal_id!, expectedDigests.node_id!);
            if (changed.changes !== 1) throw harnessError("STATE_CONFLICT", "Identity staging changed");
            db.exec("COMMIT");
          } catch (error) { db.exec("ROLLBACK"); throw error; }
        }
      } else throw harnessError("INVALID_ARGUMENT", "Invalid report spool identity encoding");
    } catch (error) { try { db.exec("ROLLBACK"); } catch {} db.close(); throw error; }
  }

  private readIdentity(): { domain_id: string; principal_id: string; node_id: string } {
    const row = this.#db.prepare("SELECT domain_id,principal_id,node_id,encoding FROM identity").get();
    if (!row) throw harnessError("INVALID_ARGUMENT", "Report spool identity is unavailable");
    if (row.encoding === "inline") return row as unknown as { domain_id: string; principal_id: string; node_id: string };
    if (row.encoding !== "chunks-v1") throw harnessError("INVALID_ARGUMENT", "Invalid report spool identity encoding");
    const result: Record<string, string> = {};
    for (const field of ["domain_id", "principal_id", "node_id"]) {
      const decoder = new InternalCanonicalJsonDecoder(); let offset = 0;
      for (;;) {
        const piece = this.#db.prepare("SELECT offset_bytes,bytes FROM identity_chunks WHERE field=? AND offset_bytes>=? ORDER BY offset_bytes LIMIT 1").get(field, offset);
        if (!piece) break;
        if (piece.offset_bytes !== offset || !(piece.bytes instanceof Uint8Array) || !piece.bytes.length || piece.bytes.length > 65_536)
          throw harnessError("INVALID_ARGUMENT", "Invalid report spool identity chunks");
        decoder.write(piece.bytes); offset += piece.bytes.length;
      }
      const value = decoder.finish();
      if (typeof value !== "string" || !value) throw harnessError("INVALID_ARGUMENT", "Invalid report spool identity");
      result[field] = value;
    }
    return result as { domain_id: string; principal_id: string; node_id: string };
  }

  private sourceKey(source: SpoolSource): string {
    const hash = createHash("sha256");
    for (const part of canonicalJsonChunks([source.attemptID, source.phase])) hash.update(part);
    return `v2:${hash.digest("hex")}`;
  }

  /** Old keys were canonical JSON TEXT. Scan those keys in bounded text
   * slices without requiring a whole-key JS string or a startup migration. */
  private hasLegacyKeyDigest(digest: string): boolean {
    let cursor = 0;
    for (;;) {
      const row = this.#db.prepare("SELECT rowid n FROM reports WHERE rowid>? AND source_format='inline' ORDER BY rowid LIMIT 1").get(cursor);
      if (!row) return false;
      cursor = Number(row.n);
      if (!Number.isSafeInteger(cursor) || cursor <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid report spool key row");
      const hash = createHash("sha256"); let position = 1;
      for (;;) {
        // SQLite TEXT substr counts Unicode characters and returns at most
        // 16,384 characters, preserving UTF-8 without allocating the full key.
        const piece = this.#db.prepare("SELECT substr(key,?,16384) part FROM reports WHERE rowid=?").get(position, cursor)?.part;
        if (typeof piece !== "string") throw harnessError("INVALID_ARGUMENT", "Invalid retained report key");
        if (!piece.length) break;
        hash.update(piece, "utf8");
        position += [...piece].length;
      }
      if (hash.digest("hex") === digest) return true;
    }
  }

  private readSource(row: Record<string, unknown>): SpoolSource {
    if (row.source_format === "inline") return parseJson(row.source as string) as unknown as SpoolSource;
    if (row.source_format !== "chunks-v1" || row.source !== "null" || row.source_ready !== 1
        || !Number.isSafeInteger(row.source_bytes) || Number(row.source_bytes) <= 0 || typeof row.source_digest !== "string"
        || !/^[a-f0-9]{64}$/.test(row.source_digest) || typeof row.key !== "string")
      throw harnessError("INVALID_ARGUMENT", "Invalid retained report source manifest");
    const decoder = new InternalCanonicalJsonDecoder(), hash = createHash("sha256"); let offset = 0;
    for (;;) {
      const piece = this.#db.prepare("SELECT offset_bytes,bytes FROM report_source_chunks WHERE report_key=? AND offset_bytes>=? ORDER BY offset_bytes LIMIT 1").get(row.key, offset);
      if (!piece) break;
      if (piece.offset_bytes !== offset || !(piece.bytes instanceof Uint8Array) || !piece.bytes.length || piece.bytes.length > 65_536)
        throw harnessError("INVALID_ARGUMENT", "Retained report source chunk is invalid");
      offset += piece.bytes.length; if (offset > Number(row.source_bytes)) throw harnessError("INVALID_ARGUMENT", "Retained report source exceeds its manifest");
      hash.update(piece.bytes); decoder.write(piece.bytes);
    }
    if (offset !== row.source_bytes || hash.digest("hex") !== row.source_digest) throw harnessError("INVALID_ARGUMENT", "Retained report source checksum mismatch");
    const source = decoder.finish() as unknown as SpoolSource;
    if (!source || typeof source !== "object" || !source.grant || this.sourceKey(source) !== row.key)
      throw harnessError("INVALID_ARGUMENT", "Retained report source identity is invalid");
    if (row.source_expires_at !== null) {
      if (!Number.isSafeInteger(row.source_expires_at) || Number(row.source_expires_at) <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid retained report expiry");
      source.grant.expiresAt = Number(row.source_expires_at);
    }
    return source;
  }

  private sourceRow(key: string): Record<string, unknown> {
    const row = this.#db.prepare("SELECT key,source,source_format,source_bytes,source_digest,source_ready,source_expires_at FROM reports WHERE key=?").get(key);
    if (!row) throw harnessError("STATE_CONFLICT", "Report reservation is unavailable");
    return row;
  }

  beginChunkedSource(key: string, sourceBytes: number, digest: string, valueLimit: number): void {
    if (!/^v2:[a-f0-9]{64}$/.test(key) || !Number.isSafeInteger(sourceBytes) || sourceBytes <= 65_536
        || !/^[a-f0-9]{64}$/.test(digest) || !Number.isSafeInteger(valueLimit) || valueLimit <= 0)
      throw harnessError("INVALID_ARGUMENT", "Invalid chunked source reservation");
    const needed = BigInt(valueLimit) + BigInt(sourceBytes) + 15n + BigInt(Buffer.byteLength(canonicalJson({ kind: "report", value: null })) - 4);
    if (needed > BigInt(this.maxBytes)) throw harnessError("LIMIT_EXCEEDED", "Report spool reservation exceeds its logical quota");
    // Exclusive spool ownership and the serial worker keep this check and
    // the subsequent insertion isolated from other spool writers.
    if (this.hasLegacyKeyDigest(key.slice(3))) throw harnessError("REQUEST_CONFLICT", "Report spool reservation already exists");
    const bytes = Number(needed);
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      if (this.#db.prepare("SELECT key FROM reports WHERE key=?").get(key)) throw harnessError("REQUEST_CONFLICT", "Report spool reservation already exists");
      const used = Number(this.#db.prepare("SELECT coalesce(sum(reserved),0) AS n FROM reports").get()!.n);
      if (used > this.maxBytes - bytes) throw harnessError("LIMIT_EXCEEDED", "Report spool logical quota is full");
      this.#db.prepare("INSERT INTO reports(key,reserved,source,report,state,content_bytes,digest,source_format,source_bytes,source_digest,source_ready) VALUES(?,?,'null',NULL,'reserved',NULL,NULL,'chunks-v1',?,?,0)")
        .run(key, bytes, sourceBytes, digest);
      this.#db.exec("COMMIT");
    } catch (error) { this.#db.exec("ROLLBACK"); throw error; }
  }

  appendSourceChunk(key: string, offset: number, bytes: Uint8Array): void {
    if (!Number.isSafeInteger(offset) || offset < 0 || !(bytes instanceof Uint8Array) || !bytes.length || bytes.length > 65_536)
      throw harnessError("INVALID_ARGUMENT", "Invalid source chunk");
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const row = this.#db.prepare("SELECT state,source_format,source_ready,source_bytes FROM reports WHERE key=?").get(key);
      if (!row || row.state !== "reserved" || row.source_format !== "chunks-v1" || row.source_ready !== 0) throw harnessError("STATE_CONFLICT", "Source staging is unavailable");
      const previous = this.#db.prepare("SELECT offset_bytes,length(bytes) n FROM report_source_chunks WHERE report_key=? ORDER BY offset_bytes DESC LIMIT 1").get(key);
      const written = previous ? Number(previous.offset_bytes) + Number(previous.n) : 0;
      if (offset !== written || offset + bytes.length > Number(row.source_bytes)) throw harnessError("INVALID_ARGUMENT", "Source chunks must be contiguous");
      this.#db.prepare("INSERT INTO report_source_chunks(report_key,offset_bytes,bytes) VALUES(?,?,?)").run(key, offset, bytes);
      this.#db.exec("COMMIT");
    } catch (error) { this.#db.exec("ROLLBACK"); throw error; }
  }

  abortChunkedSource(key: string): void {
    this.#db.prepare("DELETE FROM reports WHERE key=? AND state='reserved' AND source_format='chunks-v1' AND source_ready=0").run(key);
  }

  finishChunkedSource(key: string): void {
    // The spool owns an exclusive SQLite lock for its lifetime and the worker
    // serializes its operations, so staged chunks cannot change between this
    // bounded prevalidation and the short manifest publication transaction.
    const row = this.sourceRow(key);
    if (row.source_format !== "chunks-v1" || row.source_ready !== 0) throw harnessError("STATE_CONFLICT", "Source staging is unavailable");
    const source = this.readSource({ ...row, source_ready: 1 });
    const identity = this.readIdentity();
    if (source.nodeID !== identity.node_id || source.grant.nodeID !== identity.node_id || source.grant.workerPrincipalID !== identity.principal_id
        || source.grant.subjectID !== source.actionID || source.grant.attemptID !== source.attemptID || source.assignmentID !== source.attemptID
        || source.grant.incarnation !== source.incarnation || !source.runID?.trim()) throw harnessError("INVALID_ARGUMENT", "Retained report source is invalid");
    const { runID: _runID, ...params } = source;
    validateNodeRequest({ protocolVersion: "harness/1", requestID: "spool-validation", binding: { domainID: identity.domain_id, principalID: identity.principal_id },
      method: "reportAction", params: { ...params, report: { kind: "diagnostic", code: "CAPACITY_THROWN", phase: source.phase }, observationOnly: true } });
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const changed = this.#db.prepare("UPDATE reports SET source_ready=1 WHERE key=? AND state='reserved' AND source_format='chunks-v1' AND source_ready=0 AND source_bytes=? AND source_digest=?")
        .run(key, row.source_bytes as number, row.source_digest as string);
      if (changed.changes !== 1) throw harnessError("STATE_CONFLICT", "Source staging changed");
      this.#db.exec("COMMIT");
    } catch (error) { this.#db.exec("ROLLBACK"); throw error; }
  }

  startSourceComparison(key: string, expiresAt: number): string {
    if (!Number.isSafeInteger(expiresAt) || expiresAt <= 0 || this.#sourceComparisons.size >= 64) throw harnessError("INVALID_ARGUMENT", "Invalid source comparison");
    const source = this.readSource(this.sourceRow(key)); source.grant.expiresAt = expiresAt;
    const id = randomUUID(); this.#sourceComparisons.set(id, canonicalJsonChunks(source)[Symbol.iterator]()); return id;
  }
  compareSourceChunk(id: string, bytes: Uint8Array): void {
    const expected = this.#sourceComparisons.get(id)?.next();
    if (!expected || expected.done || !(bytes instanceof Uint8Array) || !Buffer.from(expected.value).equals(Buffer.from(bytes)))
      throw harnessError("REQUEST_CONFLICT", "Report source changed after reservation");
  }
  finishSourceComparison(id: string): void {
    const iterator = this.#sourceComparisons.get(id); this.#sourceComparisons.delete(id);
    if (!iterator || !iterator.next().done) throw harnessError("REQUEST_CONFLICT", "Report source changed after reservation");
  }
  abortSourceComparison(id: string): void { this.#sourceComparisons.delete(id); }

  async validate(binding: SessionBinding, nodeID: string): Promise<void> {
    let cursor = 0;
    for (;;) {
        const row = this.#db.prepare("SELECT rowid AS n,* FROM reports WHERE rowid>? ORDER BY rowid LIMIT 1").get(cursor);
        if (!row) break;
        cursor = Number(row.n);
        if (!Number.isSafeInteger(cursor) || cursor <= 0 || !Number.isSafeInteger(row.reserved) || Number(row.reserved) <= 0
            || typeof row.source !== "string" || !["reserved", "ready"].includes(row.state as string) || !["inline", "chunks-v1"].includes(row.format as string)
            || row.source_format === "chunks-v1" && row.state === "ready" && row.format !== "chunks-v1"
            || row.state === "reserved" && (row.report !== null || row.digest !== null || row.content_bytes !== null || row.report_digest !== null)
            || row.state === "ready" && ((row.format === "inline" ? typeof row.report !== "string" : row.report !== null)
              || typeof row.digest !== "string" || !Number.isSafeInteger(row.content_bytes)
              || row.format === "chunks-v1" && (typeof row.report_digest !== "string" || !/^[a-f0-9]{64}$/.test(row.report_digest))))
          throw harnessError("INVALID_ARGUMENT", "Invalid retained report record");
        // An interrupted, unpublished source reservation cannot be an execution
        // receipt. It is reclaimed with other reserved rows after validation.
        if (row.state === "reserved" && row.source_format === "chunks-v1" && row.source_ready === 0) continue;
        const source = this.readSource(row);
        if (!source || typeof source !== "object" || typeof source.runID !== "string" || !source.runID.trim() || !source.grant
            || source.nodeID !== nodeID || source.grant.nodeID !== nodeID || source.grant.incarnation !== source.incarnation
            || source.grant.workerPrincipalID !== binding.principalID || source.grant.subjectID !== source.actionID
            || source.grant.attemptID !== source.attemptID || source.assignmentID !== source.attemptID
            || row.key !== (row.source_format === "chunks-v1" ? this.sourceKey(source) : canonicalJson([source.attemptID, source.phase]))) throw harnessError("INVALID_ARGUMENT", "Retained report source is invalid");
        if (row.state === "ready") {
          const report = row.format === "chunks-v1" ? this.decodeChunks(row.key as string, canonicalJsonChunks(source), Number(row.content_bytes), row.digest as string, row.report_digest as string)
            : parseJson(row.report as string) as unknown as ActionReport;
          if (row.format === "inline") {
            const text = row.report as string, bytes = Buffer.byteLength(row.source) + Buffer.byteLength(text);
            if (bytes !== row.content_bytes || bytes > Number(row.reserved) || checksum(row.source, text) !== row.digest) throw harnessError("INVALID_ARGUMENT", "Retained report checksum mismatch");
          } else if (Number(row.content_bytes) > Number(row.reserved)) throw harnessError("INVALID_ARGUMENT", "Retained report exceeds reservation");
          const { runID: _runID, ...params } = source;
          validateNodeRequest({ protocolVersion: "harness/1", requestID: "spool-validation", binding, method: "reportAction", params: { ...params, report, observationOnly: true } });
        } else {
          const { runID: _runID, ...params } = source;
          validateNodeRequest({ protocolVersion: "harness/1", requestID: "spool-validation", binding, method: "reportAction",
            params: { ...params, report: { kind: "diagnostic", code: "CAPACITY_THROWN", phase: source.phase }, observationOnly: true } });
        }
        await new Promise<void>(resolve => setImmediate(resolve));
    }
    // Only reservations with no committed report are removed after every row
    // has passed validation. Their execution intent remains at the coordinator.
    this.#db.exec("DELETE FROM reports WHERE state='reserved'");
  }

  reserve(source: SpoolSource, valueLimit: number): string {
    const key = canonicalJson([source.attemptID, source.phase]);
    const shortKey = `v2:${createHash("sha256").update(key, "utf8").digest("hex")}`;
    const maximumSource = { ...source, grant: { ...source.grant, expiresAt: Number.MAX_SAFE_INTEGER } };
    const sourceText = canonicalJson(maximumSource);
    const reportOverhead = Buffer.byteLength(canonicalJson({ kind: "report", value: null })) - 4;
    const needed = BigInt(valueLimit) + BigInt(Buffer.byteLength(sourceText)) + BigInt(reportOverhead);
    if (needed > BigInt(this.maxBytes)) throw harnessError("LIMIT_EXCEEDED", "Report spool reservation exceeds its logical quota");
    const bytes = Number(needed);
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.#db.prepare("SELECT report FROM reports WHERE key IN (?,?) LIMIT 1").get(key, shortKey);
      if (existing) throw harnessError("REQUEST_CONFLICT", "Report spool reservation already exists");
      const used = Number(this.#db.prepare("SELECT coalesce(sum(reserved),0) AS n FROM reports").get()!.n);
      if (used > this.maxBytes - bytes) throw harnessError("LIMIT_EXCEEDED", "Report spool logical quota is full");
      this.#db.prepare("INSERT INTO reports(key,reserved,source,report,state,content_bytes,digest) VALUES(?,?,?,NULL,'reserved',NULL,NULL)").run(key, bytes, canonicalJson(source));
      this.#db.exec("COMMIT");
      return key;
    } catch (error) { this.#db.exec("ROLLBACK"); throw error; }
  }

  save(key: string, source: SpoolSource, report: ActionReport): void {
    const sourceText = canonicalJson(source), reportText = canonicalJson(report);
    const used = Buffer.byteLength(sourceText) + Buffer.byteLength(reportText);
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const row = this.#db.prepare("SELECT reserved,source,report,state FROM reports WHERE key=?").get(key);
      if (!row || used > Number(row.reserved)) throw harnessError("LIMIT_EXCEEDED", "Report exceeds its spool reservation");
      if (row.state === "ready" && (row.report !== reportText || row.source !== sourceText)) throw harnessError("REQUEST_CONFLICT", "Report spool content changed");
      if (row.state !== "reserved" && row.state !== "ready") throw harnessError("INVALID_ARGUMENT", "Invalid report spool state");
      this.#db.prepare("UPDATE reports SET source=?,report=?,state='ready',content_bytes=?,digest=? WHERE key=?").run(sourceText, reportText, used, checksum(sourceText, reportText), key);
      this.#db.exec("COMMIT");
    } catch (error) { this.#db.exec("ROLLBACK"); throw error; }
  }

  /** Each bounded piece is durable, but is not a recoverable report until finishChunks commits. */
  appendChunk(key: string, offset: number, bytes: Uint8Array): void {
    if (!Number.isSafeInteger(offset) || offset < 0 || !(bytes instanceof Uint8Array) || bytes.byteLength === 0 || bytes.byteLength > 65_536)
      throw harnessError("INVALID_ARGUMENT", "Invalid spool chunk");
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const row = this.#db.prepare("SELECT state,reserved,format FROM reports WHERE key=?").get(key);
      if (!row || row.state !== "reserved" && !(row.state === "ready" && row.format === "chunks-v1")) throw harnessError("STATE_CONFLICT", "Report reservation is unavailable");
      const old = this.#db.prepare("SELECT bytes FROM report_chunks WHERE report_key=? AND offset_bytes=?").get(key, offset);
      if (old) {
        if (!(old.bytes instanceof Uint8Array) || Buffer.compare(Buffer.from(old.bytes), Buffer.from(bytes)) !== 0)
          throw harnessError("REQUEST_CONFLICT", "Spool chunk replay differs from its original bytes");
      } else {
        if (row.state !== "reserved") throw harnessError("REQUEST_CONFLICT", "Completed report has no such chunk");
        const previous = this.#db.prepare("SELECT offset_bytes,length(bytes) n FROM report_chunks WHERE report_key=? ORDER BY offset_bytes DESC LIMIT 1").get(key);
        const written = previous ? Number(previous.offset_bytes) + Number(previous.n) : 0;
        if (written !== offset || offset + bytes.byteLength > Number(row.reserved)) throw harnessError("INVALID_ARGUMENT", "Spool chunks must be contiguous");
        this.#db.prepare("INSERT INTO report_chunks(report_key,offset_bytes,bytes) VALUES(?,?,?)").run(key, offset, bytes);
      }
      this.#db.exec("COMMIT");
    } catch (error) { this.#db.exec("ROLLBACK"); throw error; }
  }

  checkChunkReservation(key: string, source: SpoolSource | number, reportBytes: number): void {
    const used = (typeof source === "number" ? source : Buffer.byteLength(canonicalJson(source))) + reportBytes;
    const row = this.#db.prepare("SELECT state,reserved,format FROM reports WHERE key=?").get(key);
    if (!Number.isSafeInteger(reportBytes) || reportBytes <= 0 || !Number.isSafeInteger(used) || !row
        || used > Number(row.reserved)) throw harnessError("LIMIT_EXCEEDED", "Report exceeds its spool reservation");
    if (row.state !== "reserved" && !(row.state === "ready" && row.format === "chunks-v1"))
      throw harnessError("STATE_CONFLICT", "Report reservation is unavailable");
  }

  finishChunks(key: string, sourceInput: SpoolSource | number, reportBytes: number, digest: string, reportDigest: string): void {
    const streamedSource = typeof sourceInput === "number";
    const source = streamedSource ? this.readSource(this.sourceRow(key)) : sourceInput;
    if (streamedSource) source.grant.expiresAt = sourceInput;
    const sourceChunks = canonicalJsonChunks(source); let sourceLength = 0;
    for (const part of sourceChunks) sourceLength += part.length;
    const sourceText = streamedSource ? null : canonicalJson(source), used = sourceLength + reportBytes;
    if (!Number.isSafeInteger(reportBytes) || reportBytes <= 0 || !/^[a-f0-9]{64}$/.test(digest) || !/^[a-f0-9]{64}$/.test(reportDigest)) throw harnessError("INVALID_ARGUMENT", "Invalid chunked report");
    const report = this.decodeChunks(key, sourceChunks, used, digest, reportDigest);
    const { runID: _runID, ...params } = source;
    validateNodeRequest({ protocolVersion: "harness/1", requestID: "spool-validation", binding: { domainID: "spool-validation", principalID: source.grant.workerPrincipalID },
      method: "reportAction", params: { ...params, report, observationOnly: true } });
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const row = this.#db.prepare("SELECT state,reserved,source,source_format,source_expires_at,content_bytes,digest,format,report_digest FROM reports WHERE key=?").get(key);
      if (!row || used > Number(row.reserved)) throw harnessError("LIMIT_EXCEEDED", "Report exceeds its spool reservation");
      if (row.state === "ready") {
        if (row.format !== "chunks-v1" || (streamedSource ? row.source_format !== "chunks-v1" || row.source_expires_at !== sourceInput : row.source !== sourceText)
            || row.content_bytes !== used || row.digest !== digest || row.report_digest !== reportDigest)
          throw harnessError("REQUEST_CONFLICT", "Report spool content changed");
      } else if (row.state === "reserved") {
        this.#db.prepare("UPDATE reports SET source=?,source_expires_at=?,report=NULL,state='ready',content_bytes=?,digest=?,report_digest=?,format='chunks-v1' WHERE key=? AND state='reserved'")
          .run(sourceText ?? "null", streamedSource ? sourceInput : null, used, digest, reportDigest, key);
      } else throw harnessError("INVALID_ARGUMENT", "Invalid report spool state");
      this.#db.exec("COMMIT");
    } catch (error) { this.#db.exec("ROLLBACK"); throw error; }
  }

  private decodeChunks(key: string, source: Iterable<Uint8Array>, used: number, digest: string, reportDigest?: string): ActionReport {
    const decoder = new InternalCanonicalJsonDecoder(), hash = createHash("sha256"), bodyHash = createHash("sha256"); let sourceLength = 0;
    for (const part of source) { sourceLength += part.length; hash.update(part); }
    hash.update("\0");
    const expected = used - sourceLength; let offset = 0;
    for (;;) {
      const row = this.#db.prepare("SELECT offset_bytes,bytes FROM report_chunks WHERE report_key=? AND offset_bytes>=? ORDER BY offset_bytes LIMIT 1").get(key, offset);
      if (!row) break;
      if (row.offset_bytes !== offset || !(row.bytes instanceof Uint8Array) || !row.bytes.byteLength || row.bytes.byteLength > 65_536)
        throw harnessError("INVALID_ARGUMENT", "Retained report chunk is missing or invalid");
      hash.update(row.bytes); bodyHash.update(row.bytes); decoder.write(row.bytes); offset += row.bytes.byteLength;
      if (offset > expected) throw harnessError("INVALID_ARGUMENT", "Retained report exceeds its manifest");
    }
    if (offset !== expected || hash.digest("hex") !== digest || reportDigest !== undefined && bodyHash.digest("hex") !== reportDigest)
      throw harnessError("INVALID_ARGUMENT", "Retained report checksum mismatch");
    return decoder.finish() as unknown as ActionReport;
  }

  remove(key: string): void { this.#db.prepare("DELETE FROM reports WHERE key=?").run(key); }
  actionIDs(): string[] {
    return [...new Set(this.#db.prepare("SELECT key,source,source_format,source_bytes,source_digest,source_ready,source_expires_at FROM reports WHERE state='ready'").all().map(row => this.readSource(row).actionID))];
  }
  readyKeys(): string[] { return this.#db.prepare("SELECT key FROM reports WHERE state='ready' ORDER BY rowid").all().map(row => row.key as string); }
  headDescriptor(): SpoolHeadDescriptor | null {
    const row = this.#db.prepare("SELECT key FROM reports WHERE state='ready' ORDER BY rowid LIMIT 1").get();
    return row ? this.describeDescriptor(row.key as string) : null;
  }
  describeDescriptor(key: string): SpoolHeadDescriptor {
    const row = this.#db.prepare("SELECT * FROM reports WHERE key=? AND state='ready'").get(key);
    if (!row) throw harnessError("STATE_CONFLICT", "Retained report is unavailable");
    if (row.source_format !== "chunks-v1") return { kind: "inline", head: this.describe(key) };
    if (row.source !== "null" || row.source_ready !== 1 || row.format !== "chunks-v1"
        || !Number.isSafeInteger(row.source_bytes) || Number(row.source_bytes) <= 0 || typeof row.source_digest !== "string" || !/^[a-f0-9]{64}$/.test(row.source_digest)
        || row.source_expires_at !== null && (!Number.isSafeInteger(row.source_expires_at) || Number(row.source_expires_at) <= 0)
        || !Number.isSafeInteger(row.content_bytes) || typeof row.digest !== "string" || !/^[a-f0-9]{64}$/.test(row.digest))
      throw harnessError("INVALID_ARGUMENT", "Retained report source manifest is invalid");
    return { kind: "source-chunks", key, sourceBytes: Number(row.source_bytes), sourceDigest: row.source_digest,
      sourceExpiresAt: row.source_expires_at as number | null, contentBytes: Number(row.content_bytes), digest: row.digest };
  }
  readSourceChunk(key: string, offset: number): Uint8Array | null {
    const row = this.#db.prepare("SELECT c.bytes FROM report_source_chunks c JOIN reports r ON r.key=c.report_key WHERE c.report_key=? AND c.offset_bytes=? AND r.state='ready' AND r.source_format='chunks-v1' AND r.source_ready=1").get(key, offset);
    return row?.bytes instanceof Uint8Array ? Uint8Array.from(row.bytes) : null;
  }
  head(): SpoolHead | null {
    const row = this.#db.prepare("SELECT key FROM reports WHERE state='ready' ORDER BY rowid LIMIT 1").get();
    return row ? this.describe(row.key as string) : null;
  }
  describe(key: string): SpoolHead {
    const row = this.#db.prepare("SELECT * FROM reports WHERE key=? AND state='ready'").get(key);
    if (!row) throw harnessError("STATE_CONFLICT", "Retained report is unavailable");
    const source = this.readSource(row); let sourceLength = 0;
    for (const part of canonicalJsonChunks(source)) sourceLength += part.length;
    return row.format === "chunks-v1" ? { key: row.key as string, source, chunked: true,
      length: Number(row.content_bytes) - sourceLength, digest: row.digest as string }
      : { key: row.key as string, source, report: parseJson(row.report as string) as unknown as ActionReport };
  }
  readChunk(key: string, offset: number): Uint8Array | null {
    const row = this.#db.prepare("SELECT c.bytes FROM report_chunks c JOIN reports r ON r.key=c.report_key WHERE c.report_key=? AND c.offset_bytes=? AND r.state='ready' AND r.format='chunks-v1'").get(key, offset);
    return row?.bytes instanceof Uint8Array ? Uint8Array.from(row.bytes) : null;
  }
  describeChunks(key: string): { length: number; digest: string } {
    const row = this.#db.prepare("SELECT * FROM reports WHERE key=? AND state='ready' AND format='chunks-v1'").get(key);
    if (!row || typeof row.report_digest !== "string") throw harnessError("STATE_CONFLICT", "Chunked report is unavailable");
    let sourceLength = 0; for (const part of canonicalJsonChunks(this.readSource(row))) sourceLength += part.length;
    return { length: Number(row.content_bytes) - sourceLength, digest: row.report_digest };
  }
  next(): SpoolEntry | null {
    const head = this.head();
    if (!head) return null;
    if (!("chunked" in head)) return head;
    const source = canonicalJsonChunks(head.source); let sourceLength = 0; for (const part of source) sourceLength += part.length;
    return { key: head.key, source: head.source, report: this.decodeChunks(head.key, source, head.length + sourceLength, head.digest) };
  }
  close(): void { this.#db.close(); }
}
