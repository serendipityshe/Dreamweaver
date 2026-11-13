import { createHash, randomUUID } from "node:crypto";
import { InternalCanonicalJsonDecoder, canonicalJson, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, type Json, type ValidationAssignment, type ValidationConclusion } from "../contract/index.js";
import { ValueStore } from "./value-store.js";
import type { SQLiteStore, SQLWrite, ValuePublication } from "./sqlite.js";
type Obj = Record<string, Json>;
const object = (value: unknown): value is Obj => !!value && typeof value === "object" && !Array.isArray(value);
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Validation conclusion source is unavailable or inconsistent"); }
export interface ValidationValueSource {
  encoding: "validation-conclusion-ref-v1"; jobID: string; roundID: string; assignmentID: string; assignmentEpoch: number;
  workerPrincipalID: string; payloadDigest: string; value: ValuePublication;
}
export function validationValueSource(encoded: unknown): ValidationValueSource | null {
  const body = typeof encoded === "string" ? parseJson(encoded) : encoded;
  if (!object(body) || body.encoding !== "validation-conclusion-ref-v1") return null;
  const source = body as unknown as ValidationValueSource, ref = source.value;
  if (Object.keys(body).sort().join(",") !== "assignmentEpoch,assignmentID,encoding,jobID,payloadDigest,roundID,value,workerPrincipalID"
      || ![source.jobID, source.roundID, source.assignmentID, source.workerPrincipalID].every(value => typeof value === "string" && value.length > 0)
      || !Number.isSafeInteger(source.assignmentEpoch) || source.assignmentEpoch < 1 || typeof source.payloadDigest !== "string" || !/^[a-f0-9]{64}$/.test(source.payloadDigest)
      || !ref || Object.keys(ref).sort().join(",") !== "digest,encoding,id,length,owner" || typeof ref.id !== "string" || typeof ref.owner !== "string" || ref.encoding !== "utf8-v1"
      || !Number.isSafeInteger(ref.length) || ref.length <= 0 || typeof ref.digest !== "string" || !/^[a-f0-9]{64}$/.test(ref.digest)) invalid();
  return source;
}
export async function stageValidationValue(store: SQLiteStore, assignment: ValidationAssignment, workerPrincipalID: string, conclusion: ValidationConclusion,
  quota: number, assertCurrent: () => void): Promise<ValidationValueSource | null> {
  const epoch = store.coordinatorEpoch, values = new ValueStore(store, quota);
  const check = () => { store.assertAuthority(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Validation staging owner changed"); assertCurrent(); };
  let length = 0; const hash = createHash("sha256");
  for (const bytes of canonicalJsonChunks(conclusion)) { check(); length += bytes.length; hash.update(bytes); await new Promise<void>(resolve => setImmediate(resolve)); }
  if (!Number.isSafeInteger(length)) throw harnessError("LIMIT_EXCEEDED", "Validation conclusion exceeds supported length");
  if (length <= 16384) return null;
  const source: ValidationValueSource = { encoding: "validation-conclusion-ref-v1", jobID: assignment.jobID, roundID: assignment.roundID, assignmentID: assignment.assignmentID,
    assignmentEpoch: assignment.assignmentEpoch, workerPrincipalID, payloadDigest: canonicalJsonDigest(assignment.payload),
    value: { id: `validation-conclusion:${randomUUID()}`, owner: `validation-conclusion:${store.domainID}`, encoding: "utf8-v1", length, digest: hash.digest("hex") } };
  const ref = source.value, guards = () => [write("UPDATE validation_value_staging SET state='active' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [ref.id, epoch])];
  try {
    check(); values.begin(ref, [write("INSERT INTO validation_value_staging(value_id,coordinator_epoch,state) VALUES(?,?,?)", [ref.id, epoch, "active"])]);
    let offset = 0;
    for (const bytes of canonicalJsonChunks(conclusion)) { check(); values.append(ref.id, ref.owner, offset, bytes, guards()); offset += bytes.length; await new Promise<void>(resolve => setImmediate(resolve)); }
    check(); await values.seal(ref.id, ref.owner); check(); store.checkGuards(guards()); return source;
  } catch (error) { abandonValidationValue(store, source); throw error; }
}
export function abandonValidationValue(store: SQLiteStore, source: ValidationValueSource | null): void {
  if (!source || store.requiresRecovery) return;
  try {
    store.assertAuthority(); const row = store.query("SELECT coordinator_epoch,state FROM validation_value_staging WHERE value_id=?", [source.value.id])[0];
    if (!row || row.state !== "active" || row.coordinator_epoch !== store.coordinatorEpoch) return;
    store.commit({ operationID: `validation-conclusion.abandon:${source.value.id}`, kind: "validation_value.abandoned", subjectID: source.value.id, request: { valueID: source.value.id }, evidence: null, result: null,
      writes: [write("UPDATE validation_value_staging SET state='abandoned' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, row.coordinator_epoch as number])] });
  } catch { /* Unresolved/fenced storage owns the original publication decision. */ }
}
export function validationPublicationWrites(source: ValidationValueSource | null, epoch: number): SQLWrite[] {
  return source ? [write("DELETE FROM validation_value_staging WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, epoch])] : [];
}
export async function retireValidationValues(store: SQLiteStore): Promise<void> {
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  if (store.query("SELECT s.value_id FROM validation_value_staging s JOIN value_manifests m ON m.id=s.value_id WHERE m.state='deleting' LIMIT 1")[0]) await values.resumeUnboundRetirements();
  const rows = store.query(`SELECT m.*,s.coordinator_epoch,s.state staging_state FROM validation_value_staging s JOIN value_manifests m ON m.id=s.value_id
    WHERE m.state IN ('staging','sealed') AND (s.state='abandoned' OR s.coordinator_epoch<?) AND NOT EXISTS(SELECT 1 FROM value_references v WHERE v.value_id=m.id) LIMIT 16`, [store.coordinatorEpoch]);
  for (const row of rows) await values.retireUnbound({ id: row.id as string, owner: row.owner as string, encoding: row.encoding as string, length: row.total_bytes as number,
    digest: row.digest as string, written: row.written_bytes as number, revision: row.revision as number, state: row.state as "staging" | "sealed" },
    [write("UPDATE validation_value_staging SET state=? WHERE value_id=? AND coordinator_epoch=? AND state=?", [row.staging_state as string, row.id as string, row.coordinator_epoch as number, row.staging_state as string])]);
}
export function verifyValidationValueSource(store: SQLiteStore, encoded: unknown): ValidationValueSource {
  const source = validationValueSource(encoded); if (!source) invalid(); const ref = source.value;
  const row = store.query(`SELECT m.*,h.request_json,h.result_json,j.payload_json,j.report_json,j.assignment_id,j.assignment_epoch,j.round_id,r.principal_id
    FROM value_manifests m JOIN value_references v ON v.value_id=m.id JOIN history h ON h.operation_id=v.operation_id
    JOIN validation_jobs j ON j.id=h.subject_id JOIN prepare_rounds r ON r.id=j.round_id
    WHERE m.id=? AND h.operation_id=? AND h.type='validation.reported' AND h.subject_id=?`, [ref.id, `node.report:${canonicalJson([source.assignmentID])}`, source.jobID])[0];
  if (!row || row.owner !== ref.owner || ref.owner !== `validation-conclusion:${store.domainID}` || row.encoding !== ref.encoding || row.total_bytes !== ref.length || row.written_bytes !== ref.length
      || row.digest !== ref.digest || row.state !== "published" || row.round_id !== source.roundID || row.assignment_id !== source.assignmentID || row.assignment_epoch !== source.assignmentEpoch
      || row.report_json !== canonicalJson(source)) invalid();
  const request = parseJson(row.request_json as string), result = parseJson(row.result_json as string), payload = parseJson(row.payload_json as string);
  if (!object(request) || !object(result) || !object(request.assignment) || !object(payload) || request.principalID !== source.workerPrincipalID
      || request.assignment.jobID !== source.jobID || request.assignment.roundID !== source.roundID || request.assignment.assignmentID !== source.assignmentID || request.assignment.assignmentEpoch !== source.assignmentEpoch
      || payload.principalID !== row.principal_id || canonicalJsonDigest(payload) !== source.payloadDigest || request.conclusion !== null
      || canonicalJson(request.conclusionSource) !== canonicalJson(source) || canonicalJson(result.conclusion) !== canonicalJson(source)
      || (request.encoding === "validation-report-ref-v1" ? request.assignment.payload !== null || request.assignmentPayloadDigest !== source.payloadDigest
        : request.encoding !== undefined || canonicalJsonDigest(request.assignment.payload) !== source.payloadDigest)) invalid();
  return source;
}
function* conclusionSteps(store: SQLiteStore, encoded: unknown): Generator<void, ValidationConclusion> {
  const source = validationValueSource(encoded);
  if (!source) return (typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded)) as unknown as ValidationConclusion;
  verifyValidationValueSource(store, source);
  const hash = createHash("sha256"), decoder = new InternalCanonicalJsonDecoder(); let offset = 0;
  while (offset < source.value.length) {
    const row = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [source.value.id, offset])[0];
    if (!row || !(row.payload_blob instanceof Uint8Array) || row.payload_blob.length !== row.length_bytes || !row.payload_blob.length || row.payload_blob.length > 65536
        || row.payload_blob.length > source.value.length - offset || createHash("sha256").update(row.payload_blob).digest("hex") !== row.chunk_digest) invalid();
    decoder.write(row.payload_blob); hash.update(row.payload_blob); offset += row.payload_blob.length; yield;
  }
  if (hash.digest("hex") !== source.value.digest) invalid(); const value = decoder.finish();
  if (!object(value) || typeof value.valid !== "boolean") invalid(); return value as unknown as ValidationConclusion;
}
export function readValidationConclusion(store: SQLiteStore, encoded: unknown): ValidationConclusion {
  const steps = conclusionSteps(store, encoded); for (;;) { const step = steps.next(); if (step.done) return step.value; }
}
export async function readValidationConclusionAsync(store: SQLiteStore, encoded: unknown, assertCurrent: () => void = () => {}): Promise<ValidationConclusion> {
  const epoch = store.coordinatorEpoch, steps = conclusionSteps(store, encoded);
  for (;;) { store.assertAuthority(); assertCurrent(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Validation reader owner changed"); const step = steps.next(); if (step.done) return step.value; await new Promise<void>(resolve => setImmediate(resolve)); }
}
export const validationValueMigrationSQL = `
CREATE TABLE validation_value_staging(value_id TEXT PRIMARY KEY REFERENCES value_manifests(id) ON DELETE CASCADE,coordinator_epoch INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN ('active','abandoned'))) STRICT;
CREATE TRIGGER validation_value_staging_epoch BEFORE INSERT ON validation_value_staging WHEN NEW.coordinator_epoch<>(SELECT coordinator_epoch FROM domain WHERE singleton=1) BEGIN SELECT RAISE(ABORT,'stale validation staging epoch'); END;
DROP TRIGGER validation_report_source_guard;
CREATE TRIGGER validation_report_source_guard BEFORE INSERT ON history
WHEN json_extract(NEW.request_json,'$.encoding')='validation-report-ref-v1' AND (
 NEW.type<>'validation.reported' OR NEW.operation_id IS NULL
 OR json_type(NEW.request_json,'$.assignment.payload') IS NOT 'null'
 OR json_type(NEW.request_json,'$.assignmentPayloadDigest') IS NOT 'text'
 OR NOT EXISTS (SELECT 1 FROM validation_jobs j WHERE j.id=NEW.subject_id AND j.state='accepted'
  AND j.id=json_extract(NEW.request_json,'$.assignment.jobID') AND j.round_id=json_extract(NEW.request_json,'$.assignment.roundID')
  AND j.assignment_id=json_extract(NEW.request_json,'$.assignment.assignmentID') AND j.assignment_epoch=json_extract(NEW.request_json,'$.assignment.assignmentEpoch')
  AND j.report_json=CASE WHEN json_extract(NEW.request_json,'$.conclusionSource.encoding')='validation-conclusion-ref-v1'
    THEN json_extract(NEW.request_json,'$.conclusionSource') ELSE json_extract(NEW.request_json,'$.conclusion') END)
)
BEGIN SELECT RAISE(ABORT, 'invalid validation report source'); END;
CREATE TRIGGER validation_conclusion_source_guard BEFORE INSERT ON history
WHEN json_extract(NEW.request_json,'$.conclusionSource.encoding')='validation-conclusion-ref-v1' AND (
 NEW.type<>'validation.reported' OR NEW.operation_id IS NULL OR json_type(NEW.request_json,'$.conclusion') IS NOT 'null'
 OR json_extract(NEW.result_json,'$.conclusion')<>json_extract(NEW.request_json,'$.conclusionSource')
 OR NOT EXISTS (SELECT 1 FROM validation_jobs j JOIN node_sessions n ON n.incarnation=j.node_incarnation
  JOIN value_manifests m ON m.id=json_extract(NEW.request_json,'$.conclusionSource.value.id')
  WHERE j.id=NEW.subject_id AND j.state='accepted' AND j.id=json_extract(NEW.request_json,'$.conclusionSource.jobID')
   AND j.round_id=json_extract(NEW.request_json,'$.conclusionSource.roundID') AND j.assignment_id=json_extract(NEW.request_json,'$.conclusionSource.assignmentID')
   AND j.assignment_epoch=json_extract(NEW.request_json,'$.conclusionSource.assignmentEpoch') AND n.principal_id=json_extract(NEW.request_json,'$.conclusionSource.workerPrincipalID')
   AND j.report_json=json_extract(NEW.request_json,'$.conclusionSource') AND m.state='published'
   AND m.owner=json_extract(NEW.request_json,'$.conclusionSource.value.owner') AND m.encoding='utf8-v1'
   AND m.total_bytes=json_extract(NEW.request_json,'$.conclusionSource.value.length') AND m.written_bytes=m.total_bytes
   AND m.digest=json_extract(NEW.request_json,'$.conclusionSource.value.digest'))
)
BEGIN SELECT RAISE(ABORT, 'invalid validation conclusion source'); END;
`;
