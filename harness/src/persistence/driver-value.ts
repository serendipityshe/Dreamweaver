import { createHash } from "node:crypto";
import { InternalCanonicalJsonDecoder, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, validateDecision, type Decision, type DriverConclusion, type Json } from "../contract/index.js";
import type { SQLiteStore, ValuePublication } from "./sqlite.js";
import { ValueStore } from "./value-store.js";

type Source = { encoding: "driver-conclusion-ref-v1"; frameID: string; value: ValuePublication };
const invalid = (): never => { throw harnessError("INTERNAL_ERROR", "Invalid Driver conclusion source"); };
export function driverValueSource(encoded: unknown): Source | null {
  const value = typeof encoded === "string" ? parseJson(encoded) : encoded;
  if (!value || typeof value !== "object" || Array.isArray(value) || !("encoding" in value) || value.encoding !== "driver-conclusion-ref-v1") return null;
  const source = value as Source, ref = source.value;
  if (typeof source.frameID !== "string" || !ref || typeof ref.id !== "string" || typeof ref.owner !== "string" || ref.encoding !== "utf8-v1"
      || !Number.isSafeInteger(ref.length) || ref.length <= 0 || !/^[a-f0-9]{64}$/.test(ref.digest)) invalid();
  return source;
}

/** Large dispatch/complete decisions have one canonical source. Stage bounded bytes
 * before the existing lease-guarded Driver report publication transaction. */
export async function stageDriverValue(store: SQLiteStore, frameID: string, assignmentID: string, operationID: string, conclusion: DriverConclusion, quota: number, reauthorize: () => void): Promise<Source | null> {
  if (!conclusion.ok || !conclusion.decision || typeof conclusion.decision !== "object" || Array.isArray(conclusion.decision)
      || conclusion.decision.kind !== "complete" && conclusion.decision.kind !== "dispatch") return null;
  let length = 0; const hash = createHash("sha256"), epoch = store.coordinatorEpoch;
  for (const part of canonicalJsonChunks(conclusion)) { reauthorize(); hash.update(part); length += part.length; await new Promise<void>(resolve => setImmediate(resolve)); }
  if (length <= 16_384) return null;
  const ref: ValuePublication = { id: `driver:${canonicalJsonDigest(operationID)}`, owner: `driver:${canonicalJsonDigest([store.domainID, frameID, operationID])}`,
    encoding: "utf8-v1", length, digest: hash.digest("hex") };
  const values = new ValueStore(store, quota);
  values.begin(ref, [{ sql: "INSERT INTO driver_value_staging(value_id,frame_id,assignment_id) VALUES(?,?,?)", params: [ref.id, frameID, assignmentID], expectChanges: 1 }]);
  let offset = 0;
  for (const bytes of canonicalJsonChunks(conclusion)) {
    if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Driver staging authority changed");
    reauthorize(); values.append(ref.id, ref.owner, offset, bytes); offset += bytes.length;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  reauthorize(); await values.seal(ref.id, ref.owner);
  reauthorize();
  if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Driver staging authority changed");
  return { encoding: "driver-conclusion-ref-v1", frameID, value: ref };
}

/** Only a permanently replaced/finished assignment authorizes retirement.
 * Publication and this CAS race in SQLite; time and epoch alone are not proof. */
export async function retireDriverValues(store: SQLiteStore): Promise<void> {
  store.assertAuthority();
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  if (store.query("SELECT s.value_id FROM driver_value_staging s JOIN value_manifests m ON m.id=s.value_id WHERE m.state='deleting' LIMIT 1")[0])
    await values.resumeUnboundRetirements();
  const rows = store.query(`SELECT s.value_id,s.frame_id,j.state job_state,j.assignment_id current_assignment,r.id run_id,r.business_terminal,
    m.owner,m.encoding,m.total_bytes,m.digest,m.written_bytes,m.revision,m.state
    FROM driver_value_staging s JOIN driver_jobs j ON j.id=s.frame_id JOIN runs r ON r.id=j.run_id JOIN value_manifests m ON m.id=s.value_id
    WHERE m.state IN ('staging','sealed') AND (r.business_terminal=1 OR j.state<>'assigned' OR j.assignment_id IS NOT s.assignment_id)
      AND NOT EXISTS (SELECT 1 FROM value_references r WHERE r.value_id=m.id) ORDER BY s.value_id LIMIT 16`);
  for (const row of rows) {
    await values.retireUnbound({ id: row.value_id as string, owner: row.owner as string, encoding: row.encoding as string,
      length: row.total_bytes as number, digest: row.digest as string, written: row.written_bytes as number, revision: row.revision as number, state: row.state as "staging" | "sealed" },
      row.business_terminal === 1
        ? [{ sql: "UPDATE runs SET business_terminal=? WHERE id=? AND business_terminal=?", params: [1, row.run_id as string, 1], expectChanges: 1 }]
        : [{ sql: "UPDATE driver_jobs SET state=? WHERE id=? AND state=? AND assignment_id IS ?",
          params: [row.job_state as string, row.frame_id as string, row.job_state as string, row.current_assignment as string | null], expectChanges: 1 }]);
  }
}

/** This only verifies retained content, never grants execution authority.
 * Startup uses it before taking authority; ordinary/recovery callers retain
 * their respective gates, just as for completed frame result sources. */
export function readDriverValue(store: SQLiteStore, encoded: unknown): DriverConclusion {
  const steps = readDriverValueSteps(store, encoded);
  for (;;) { const step = steps.next(); if (step.done) return step.value; }
}

/** Normal execution yields between bounded reads. Recovery/startup retains
 * the synchronous evidence reader and its separate authority gates. */
export async function readDriverValueAsync(store: SQLiteStore, encoded: unknown, evidence = false): Promise<DriverConclusion> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assert();
  const epoch = store.coordinatorEpoch, steps = readDriverValueSteps(store, encoded);
  for (;;) {
    assert();
    if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Driver source read authority changed");
    const step = steps.next();
    if (step.done) return step.value;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
}

function* readDriverValueSteps(store: SQLiteStore, encoded: unknown): Generator<void, DriverConclusion> {
  const source = driverValueSource(encoded);
  if (!source) return parseJson(encoded as string) as unknown as DriverConclusion;
  const ref = verifyDriverValueSource(store, source);
  const decoder = new InternalCanonicalJsonDecoder(), hash = createHash("sha256"); let offset = 0;
  while (offset < ref.length) {
    const chunk = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [ref.id, offset])[0];
    if (!chunk || !(chunk.payload_blob instanceof Uint8Array) || chunk.payload_blob.length !== chunk.length_bytes || !chunk.payload_blob.length
        || chunk.payload_blob.length > 65_536 || chunk.payload_blob.length > ref.length - offset
        || createHash("sha256").update(chunk.payload_blob).digest("hex") !== chunk.chunk_digest) invalid();
    const bytes = chunk!.payload_blob as Uint8Array;
    decoder.write(bytes); hash.update(bytes); offset += bytes.length;
    yield;
  }
  if (hash.digest("hex") !== ref.digest) invalid();
  const conclusion = decoder.finish();
  if (!conclusion || typeof conclusion !== "object" || Array.isArray(conclusion) || conclusion.ok !== true || !Object.hasOwn(conclusion, "decision")) invalid();
  return conclusion as unknown as DriverConclusion;
}

export function verifyDriverValueSource(store: SQLiteStore, input: unknown): ValuePublication {
  const source = driverValueSource(input);
  if (!source) invalid();
  const ref = source!.value;
  const row = store.query(`SELECT m.owner,m.encoding,m.total_bytes,m.digest,m.written_bytes,m.state,h.type,h.subject_id,h.request_json
    FROM value_manifests m JOIN value_references v ON v.value_id=m.id JOIN history h ON h.operation_id=v.operation_id WHERE m.id=?`, [ref.id])[0];
  if (!row || row.owner !== ref.owner || row.encoding !== ref.encoding || row.total_bytes !== ref.length || row.written_bytes !== ref.length
      || row.digest !== ref.digest || row.state !== "published" || row.type !== "driver.reported" || row.subject_id !== source!.frameID) invalid();
  const request = parseJson(row.request_json as string) as Record<string, Json>;
  if (request.conclusion !== null || canonicalJsonDigest(request.conclusionSource) !== canonicalJsonDigest(source)) invalid();
  return ref;
}

export function readFrameDecision(store: SQLiteStore, frameID: string, encoded: unknown): Decision {
  const source = driverValueSource(encoded);
  if (!source) return validateDecision(parseJson(encoded as string));
  if (source.frameID !== frameID) invalid();
  const report = readDriverValue(store, encoded);
  if (!report.ok) invalid();
  return validateDecision((report as Extract<DriverConclusion, { ok: true }>).decision);
}

export async function readFrameDecisionAsync(store: SQLiteStore, frameID: string, encoded: unknown, evidence = false): Promise<Decision> {
  const source = driverValueSource(encoded);
  if (!source) return validateDecision(parseJson(encoded as string));
  if (source.frameID !== frameID) invalid();
  const report = await readDriverValueAsync(store, encoded, evidence);
  if (!report.ok) invalid();
  return validateDecision((report as Extract<DriverConclusion, { ok: true }>).decision);
}

export function driverDecisionStorage(reportJSON: unknown, decision: Decision): Json {
  return copyJson(driverValueSource(reportJSON) ?? decision);
}
