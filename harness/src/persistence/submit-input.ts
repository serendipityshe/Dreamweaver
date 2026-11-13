import { createHash, randomUUID } from "node:crypto";
import { InternalCanonicalJsonDecoder, canonicalJson, canonicalJsonChunks, copyJson, harnessError, parseJson, type Json, type SubmitRequest } from "../contract/index.js";
import { ValueStore } from "./value-store.js";
import type { SQLiteStore, SQLWrite, ValuePublication } from "./sqlite.js";
type Obj = Record<string, Json>;
const object = (v: unknown): v is Obj => !!v && typeof v === "object" && !Array.isArray(v);
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid submit input source"); }
export interface SubmitInputSource { encoding: "submit-input-v1"; principalID: string; requestKey: string; value: ValuePublication }
export async function stageSubmitInput(store: SQLiteStore, principalID: string, request: SubmitRequest, quota: number, assertCaller: () => void): Promise<SubmitInputSource | null> {
  const fields = copyJson({ input: request.input, config: request.config }), epoch = store.coordinatorEpoch;
  const check = () => { store.assertAuthority(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Submit staging owner changed"); assertCaller(); };
  let length = 0; const hash = createHash("sha256");
  for (const bytes of canonicalJsonChunks(fields)) { check(); length += bytes.length; hash.update(bytes); await new Promise<void>(r => setImmediate(r)); }
  if (!Number.isSafeInteger(length)) throw harnessError("LIMIT_EXCEEDED", "Submit input exceeds supported length");
  if (length <= 16384) return null;
  const ref: ValuePublication = { id: `submit-input:${randomUUID()}`, owner: `submit-input:${store.domainID}`, encoding: "utf8-v1", length, digest: hash.digest("hex") };
  const source: SubmitInputSource = { encoding: "submit-input-v1", principalID, requestKey: request.requestKey, value: ref };
  const values = new ValueStore(store, quota);
  const guards = () => [write("UPDATE submit_input_staging SET state='active' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [ref.id, epoch])];
  try {
    check(); values.begin(ref, [write("INSERT INTO submit_input_staging(value_id,coordinator_epoch,state) VALUES(?,?,?)", [ref.id, epoch, "active"])]);
    let offset = 0;
    for (const bytes of canonicalJsonChunks(fields)) { check(); values.append(ref.id, ref.owner, offset, bytes, guards()); offset += bytes.length; await new Promise<void>(r => setImmediate(r)); }
    check(); await values.seal(ref.id, ref.owner); check(); store.checkGuards(guards());
    return source;
  } catch (error) { abandonSubmitInput(store, source); throw error; }
}
export function abandonSubmitInput(store: SQLiteStore, source: SubmitInputSource | null): void {
  if (!source || store.requiresRecovery) return;
  try {
    store.assertAuthority();
    const row = store.query("SELECT coordinator_epoch,state FROM submit_input_staging WHERE value_id=?", [source.value.id])[0];
    if (!row || row.state !== "active" || row.coordinator_epoch !== store.coordinatorEpoch) return;
    store.commit({ operationID: `submit-input.abandon:${source.value.id}`, kind: "submit_input.abandoned", subjectID: source.value.id, request: { valueID: source.value.id },
      writes: [write("UPDATE submit_input_staging SET state='abandoned' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, row.coordinator_epoch as number])], evidence: null, result: null });
  } catch { /* Recovery/fencing owns unresolved storage; never replace its error. */ }
}
export function submitPublicationWrites(source: SubmitInputSource | null, epoch: number): SQLWrite[] {
  return source ? [write("DELETE FROM submit_input_staging WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, epoch])] : [];
}
export async function retireSubmitInputs(store: SQLiteStore): Promise<void> {
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  if (store.query("SELECT s.value_id FROM submit_input_staging s JOIN value_manifests m ON m.id=s.value_id WHERE m.state='deleting' LIMIT 1")[0]) await values.resumeUnboundRetirements();
  const rows = store.query(`SELECT m.*,s.coordinator_epoch,s.state staging_state FROM submit_input_staging s JOIN value_manifests m ON m.id=s.value_id
    WHERE m.state IN ('staging','sealed') AND (s.state='abandoned' OR s.coordinator_epoch<?) AND NOT EXISTS(SELECT 1 FROM value_references v WHERE v.value_id=m.id) LIMIT 16`, [store.coordinatorEpoch]);
  for (const row of rows) await values.retireUnbound({ id: row.id as string, owner: row.owner as string, encoding: row.encoding as string, length: row.total_bytes as number,
    digest: row.digest as string, written: row.written_bytes as number, revision: row.revision as number, state: row.state as "staging" | "sealed" },
    [write("UPDATE submit_input_staging SET state=? WHERE value_id=? AND coordinator_epoch=? AND state=?", [row.staging_state as string, row.id as string, row.coordinator_epoch as number, row.staging_state as string])]);
}
export function encodeSubmitRequest(request: SubmitRequest, source: SubmitInputSource | null): Json {
  return source ? copyJson({ ...request, input: null, config: null, inputSource: source }) : copyJson(request);
}
export function submitRequestMetadata(encoded: unknown): SubmitRequest {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded); if (!object(body)) invalid();
  const { inputSource: source, ...metadata } = body;
  if (source !== undefined && (!object(source) || source.encoding !== "submit-input-v1" || typeof source.principalID !== "string" || source.requestKey !== body.requestKey || !object(source.value) || body.input !== null || body.config !== null)) invalid();
  return metadata as unknown as SubmitRequest;
}
export function verifySubmitInput(store: SQLiteStore, principalID: string, key: string, encoded?: unknown): SubmitInputSource | null {
  const row = store.query("SELECT request_json FROM commands WHERE principal_id=? AND request_key=? AND operation='submit'", [principalID, key])[0];
  if (!row || encoded !== undefined && canonicalJson(typeof encoded === "string" ? parseJson(encoded) : encoded) !== row.request_json) invalid();
  const body = parseJson(row.request_json as string) as Obj; submitRequestMetadata(body);
  if (body.inputSource === undefined) return null;
  const source = body.inputSource as unknown as SubmitInputSource, ref = source.value;
  if (source.principalID !== principalID || source.requestKey !== key || Object.keys(source).sort().join(",") !== "encoding,principalID,requestKey,value"
      || !ref || Object.keys(ref).sort().join(",") !== "digest,encoding,id,length,owner" || typeof ref.id !== "string" || typeof ref.owner !== "string"
      || ref.encoding !== "utf8-v1" || !Number.isSafeInteger(ref.length) || ref.length <= 0 || typeof ref.digest !== "string" || !/^[a-f0-9]{64}$/.test(ref.digest)) invalid();
  const proof = store.query(`SELECT m.*,h.request_json FROM value_manifests m JOIN value_references v ON v.value_id=m.id JOIN history h ON h.operation_id=v.operation_id
    WHERE m.id=? AND h.type='submit.registered' AND h.subject_id=?`, [ref.id, `command:${canonicalJson([principalID, key])}`])[0];
  if (!proof || proof.owner !== ref.owner || proof.encoding !== ref.encoding || proof.total_bytes !== ref.length || proof.written_bytes !== ref.length || proof.digest !== ref.digest || proof.state !== "published") invalid();
  const receipt = parseJson(proof.request_json as string) as Obj;
  if (canonicalJson(receipt.request) !== row.request_json) invalid();
  return source;
}
function* inputSteps(store: SQLiteStore, source: SubmitInputSource): Generator<void, { input: Json; config: Json }> {
  const hash = createHash("sha256"), decoder = new InternalCanonicalJsonDecoder(); let offset = 0;
  while (offset < source.value.length) {
    const row = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [source.value.id, offset])[0];
    if (!row || !(row.payload_blob instanceof Uint8Array) || row.payload_blob.length !== row.length_bytes || !row.payload_blob.length || row.payload_blob.length > 65536
        || row.payload_blob.length > source.value.length - offset || createHash("sha256").update(row.payload_blob).digest("hex") !== row.chunk_digest) invalid();
    decoder.write(row.payload_blob); hash.update(row.payload_blob); offset += row.payload_blob.length; yield;
  }
  if (hash.digest("hex") !== source.value.digest) invalid();
  const fields = decoder.finish(); if (!object(fields) || Object.keys(fields).sort().join(",") !== "config,input") invalid();
  return fields as { input: Json; config: Json };
}
export function readSubmitRequest(store: SQLiteStore, principalID: string, key: string, encoded: unknown): SubmitRequest {
  const metadata = submitRequestMetadata(encoded), source = verifySubmitInput(store, principalID, key, encoded);
  if (!source) return metadata;
  const steps = inputSteps(store, source); for (;;) { const step = steps.next(); if (step.done) return { ...metadata, ...step.value }; }
}
export async function readSubmitFields(store: SQLiteStore, source: SubmitInputSource, evidence = false): Promise<{ input: Json; config: Json }> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority(); assert(); const epoch = store.coordinatorEpoch;
  const actual = verifySubmitInput(store, source.principalID, source.requestKey);
  if (canonicalJson(actual) !== canonicalJson(source)) invalid();
  const steps = inputSteps(store, source);
  for (;;) { assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Submit reader owner changed"); const step = steps.next(); if (step.done) return step.value; await new Promise<void>(r => setImmediate(r)); }
}
export function submitValidationPayload(request: SubmitRequest, principalID: string, ref: { id: string; version: string }, revision: number | null, encoded?: unknown): Json {
  const body = encoded === undefined ? copyJson(request) : typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded);
  const source = (body as Obj).inputSource;
  return copyJson({ kind: "agent", ref, phase: "input", value: source ? null : request.input, config: source ? null : request.config,
    principalID, operation: "client.submit", context: { request: body, pointerRevision: revision, ...(source ? { submitSource: source } : {}) } });
}
export function rootInputBinding(store: SQLiteStore, value: Obj): SubmitInputSource | null {
  if (!object(value.inputSource) || value.inputSource.encoding !== "root-submit-input-v1") return null;
  const source = value.inputSource;
  if (Object.keys(source).sort().join(",") !== "digest,encoding,principalID,requestKey" || typeof source.principalID !== "string" || typeof source.requestKey !== "string"
      || value.parentActionID !== null || value.rootID !== value.id || typeof value.id !== "string" || value.input !== null || value.config !== null) invalid();
  const row = store.query(`SELECT c.request_json,c.result_json FROM root_run_inputs b JOIN commands c ON c.principal_id=b.principal_id AND c.request_key=b.request_key
    JOIN runs r ON r.id=b.run_id AND r.principal_id=b.principal_id WHERE b.run_id=? AND b.principal_id=? AND b.request_key=? AND c.operation='submit' AND c.status='applied'`, [value.id, source.principalID, source.requestKey])[0];
  if (!row) invalid(); const result = parseJson(row.result_json as string) as Obj;
  if (result.runID !== value.id || canonicalJson(result.agent) !== canonicalJson(value.agent)) invalid();
  const ref = verifySubmitInput(store, source.principalID, source.requestKey, row.request_json);
  if (!ref || ref.value.digest !== source.digest) invalid(); return ref;
}
export function rootInputSource(source: SubmitInputSource): Json { return { encoding: "root-submit-input-v1", principalID: source.principalID, requestKey: source.requestKey, digest: source.value.digest }; }
export function rootFieldsSync(store: SQLiteStore, source: SubmitInputSource): { input: Json; config: Json } {
  const steps = inputSteps(store, source); for (;;) { const step = steps.next(); if (step.done) return step.value; }
}
