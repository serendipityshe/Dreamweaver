import { createHash, randomUUID } from "node:crypto";
import { InternalCanonicalJsonDecoder, canonicalJson, canonicalJsonChunks, copyJson, harnessError, parseJson, type Checkpoint, type Json } from "../contract/index.js";
import { ValueStore } from "./value-store.js";
import type { SQLiteStore, SQLWrite, ValuePublication } from "./sqlite.js";

type Obj = Record<string, Json>;
const object = (value: unknown): value is Obj => !!value && typeof value === "object" && !Array.isArray(value);
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Checkpoint source evidence is unavailable or inconsistent"); }
export interface CheckpointValueSource {
  encoding: "checkpoint-value-v1"; actionID: string; attemptID: string; operationID: string; revision: number; value: ValuePublication;
}
// The complete native checkpoint is internal and immutable. Its provenance is
// remembered without adding private fields to callbacks or public views.
const origins = new WeakMap<Checkpoint, CheckpointValueSource>();
export function checkpointOperationID(attemptID: string, operationID: string): string {
  const hash = createHash("sha256"); for (const bytes of canonicalJsonChunks([attemptID, operationID])) hash.update(bytes);
  return `action.checkpoint:${hash.digest("hex")}`;
}
export function checkpointHasLargeValue(value: Json): boolean {
  let bytes = 0; for (const part of canonicalJsonChunks(value)) { bytes += part.length; if (bytes > 16384) return true; } return false;
}
export function checkpointWithSource(source: CheckpointValueSource, value: Json): Checkpoint {
  const checkpoint = { revision: source.revision, value }; origins.set(checkpoint, source); return checkpoint;
}
export function encodeCheckpoint(checkpoint: Checkpoint): Json {
  const source = origins.get(checkpoint);
  return source ? copyJson({ revision: checkpoint.revision, value: null, valueSource: source }) : copyJson(checkpoint);
}
export function encodeCheckpointFields<T>(input: T): T {
  if (!object(input) || !object(input.checkpoint)) return input;
  const source = origins.get(input.checkpoint as unknown as Checkpoint);
  return source ? { ...input, checkpoint: encodeCheckpoint(input.checkpoint as unknown as Checkpoint) } as T : input;
}

export async function stageCheckpointValue(store: SQLiteStore, binding: Omit<CheckpointValueSource, "encoding" | "value">, value: Json,
  quota: number, assertCurrent: () => void): Promise<CheckpointValueSource> {
  const epoch = store.coordinatorEpoch, values = new ValueStore(store, quota);
  const check = () => { store.assertAuthority(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Checkpoint staging owner changed"); assertCurrent(); };
  let length = 0; const hash = createHash("sha256");
  for (const bytes of canonicalJsonChunks(value)) { check(); length += bytes.length; hash.update(bytes); await new Promise<void>(resolve => setImmediate(resolve)); }
  if (!Number.isSafeInteger(length)) throw harnessError("LIMIT_EXCEEDED", "Checkpoint exceeds supported length");
  const source: CheckpointValueSource = { encoding: "checkpoint-value-v1", ...binding,
    value: { id: `checkpoint:${randomUUID()}`, owner: `checkpoint:${store.domainID}`, encoding: "utf8-v1", length, digest: hash.digest("hex") } };
  const ref = source.value, guards = () => [write("UPDATE checkpoint_value_staging SET state='active' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [ref.id, epoch])];
  try {
    check(); values.begin(ref, [write("INSERT INTO checkpoint_value_staging(value_id,coordinator_epoch,state) VALUES(?,?,?)", [ref.id, epoch, "active"])]);
    let offset = 0;
    for (const bytes of canonicalJsonChunks(value)) { check(); values.append(ref.id, ref.owner, offset, bytes, guards()); offset += bytes.length; await new Promise<void>(resolve => setImmediate(resolve)); }
    check(); await values.seal(ref.id, ref.owner); check(); store.checkGuards(guards()); return source;
  } catch (error) { abandonCheckpointValue(store, source); throw error; }
}
export function abandonCheckpointValue(store: SQLiteStore, source: CheckpointValueSource): void {
  if (store.requiresRecovery) return;
  try {
    store.assertAuthority(); const row = store.query("SELECT coordinator_epoch,state FROM checkpoint_value_staging WHERE value_id=?", [source.value.id])[0];
    if (!row || row.state !== "active" || row.coordinator_epoch !== store.coordinatorEpoch) return;
    store.commit({ operationID: `checkpoint.abandon:${source.value.id}`, kind: "checkpoint_value.abandoned", subjectID: source.value.id, request: { valueID: source.value.id }, evidence: null, result: null,
      writes: [write("UPDATE checkpoint_value_staging SET state='abandoned' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, row.coordinator_epoch as number])] });
  } catch { /* A frozen/fenced connection cannot retire an unresolved source. */ }
}
export function checkpointPublicationWrites(source: CheckpointValueSource, epoch: number): SQLWrite[] {
  return [write("DELETE FROM checkpoint_value_staging WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, epoch])];
}
export async function retireCheckpointValues(store: SQLiteStore): Promise<void> {
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  if (store.query("SELECT s.value_id FROM checkpoint_value_staging s JOIN value_manifests m ON m.id=s.value_id WHERE m.state='deleting' LIMIT 1")[0]) await values.resumeUnboundRetirements();
  const rows = store.query(`SELECT m.*,s.coordinator_epoch,s.state staging_state FROM checkpoint_value_staging s JOIN value_manifests m ON m.id=s.value_id
    WHERE m.state IN ('staging','sealed') AND (s.state='abandoned' OR s.coordinator_epoch<?) AND NOT EXISTS(SELECT 1 FROM value_references v WHERE v.value_id=m.id) LIMIT 16`, [store.coordinatorEpoch]);
  for (const row of rows) await values.retireUnbound({ id: row.id as string, owner: row.owner as string, encoding: row.encoding as string, length: row.total_bytes as number,
    digest: row.digest as string, written: row.written_bytes as number, revision: row.revision as number, state: row.state as "staging" | "sealed" },
    [write("UPDATE checkpoint_value_staging SET state=? WHERE value_id=? AND coordinator_epoch=? AND state=?", [row.staging_state as string, row.id as string, row.coordinator_epoch as number, row.staging_state as string])]);
}
function sourceFor(store: SQLiteStore, actionID: string, encoded: Json): CheckpointValueSource | null {
  if (!object(encoded) || !Number.isSafeInteger(encoded.revision) || typeof encoded.revision !== "number" || encoded.revision < 0 || !Object.hasOwn(encoded, "value")) invalid();
  if (!Object.hasOwn(encoded, "valueSource")) return null;
  const source = encoded.valueSource as unknown as CheckpointValueSource, ref = source?.value;
  if (encoded.value !== null || !source || Object.keys(source).sort().join(",") !== "actionID,attemptID,encoding,operationID,revision,value"
      || source.encoding !== "checkpoint-value-v1" || source.actionID !== actionID || typeof source.attemptID !== "string" || !source.attemptID
      || typeof source.operationID !== "string" || !source.operationID || source.revision !== encoded.revision || source.revision < 1
      || !ref || Object.keys(ref).sort().join(",") !== "digest,encoding,id,length,owner" || ref.encoding !== "utf8-v1" || typeof ref.id !== "string"
      || ref.owner !== `checkpoint:${store.domainID}` || !Number.isSafeInteger(ref.length) || ref.length <= 0 || typeof ref.digest !== "string" || !/^[a-f0-9]{64}$/.test(ref.digest)) invalid();
  const receipt = store.query(`SELECT h.request_json,h.result_json,h.data_json,m.owner,m.encoding,m.total_bytes,m.written_bytes,m.digest,m.state,
      b.number attempt_number,b.epoch attempt_epoch,b.node_id attempt_node,b.incarnation attempt_incarnation,b.worker_principal_id attempt_principal
    FROM history h JOIN value_references v ON v.operation_id=h.operation_id JOIN value_manifests m ON m.id=v.value_id
    JOIN actions a ON a.id=h.subject_id JOIN business_attempts b ON b.id=? AND b.action_id=a.id
    WHERE h.operation_id=? AND h.type='action.checkpoint' AND h.subject_id=? AND m.id=?`,
    [source.attemptID, checkpointOperationID(source.attemptID, source.operationID), actionID, ref.id])[0];
  if (!receipt || receipt.owner !== ref.owner || receipt.encoding !== ref.encoding || receipt.total_bytes !== ref.length || receipt.written_bytes !== ref.length
      || receipt.digest !== ref.digest || receipt.state !== "published" || receipt.result_json !== canonicalJson(encoded) || receipt.data_json !== receipt.result_json) invalid();
  const request = parseJson(receipt.request_json as string);
  if (!object(request) || request.actionID !== actionID || request.attemptID !== source.attemptID || request.operationID !== source.operationID
      || request.expectedRevision !== source.revision - 1 || request.value !== null || canonicalJson(request.valueSource) !== canonicalJson(source)
      || !object(request.grant) || request.grant.kind !== "action" || request.grant.subjectID !== actionID || request.grant.attemptID !== source.attemptID
      || request.grant.attemptNumber !== receipt.attempt_number || request.grant.epoch !== receipt.attempt_epoch || request.grant.nodeID !== receipt.attempt_node
      || request.grant.incarnation !== receipt.attempt_incarnation || request.grant.workerPrincipalID !== receipt.attempt_principal) invalid();
  return source;
}
/** Conservative full-value response accounting without hydrating the value. */
export function checkpointValueByteDelta(store: SQLiteStore, actionID: string, encoded: Json): number {
  const source = sourceFor(store, actionID, encoded); if (!source) return 0;
  return source.value.length + Buffer.byteLength(canonicalJson({ revision: source.revision, value: null }), "utf8") - 4 - Buffer.byteLength(canonicalJson(encoded), "utf8");
}
function* valueSteps(store: SQLiteStore, source: CheckpointValueSource): Generator<void, Json> {
  const hash = createHash("sha256"), decoder = new InternalCanonicalJsonDecoder(); let offset = 0;
  while (offset < source.value.length) {
    const row = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [source.value.id, offset])[0];
    if (!row || !(row.payload_blob instanceof Uint8Array) || row.payload_blob.length !== row.length_bytes || !row.payload_blob.length || row.payload_blob.length > 65536
        || row.payload_blob.length > source.value.length - offset || createHash("sha256").update(row.payload_blob).digest("hex") !== row.chunk_digest) invalid();
    decoder.write(row.payload_blob); hash.update(row.payload_blob); offset += row.payload_blob.length; yield;
  }
  if (hash.digest("hex") !== source.value.digest) invalid(); return decoder.finish();
}
export function readCheckpointValue(store: SQLiteStore, actionID: string, encoded: Json): Checkpoint {
  const source = sourceFor(store, actionID, encoded); if (!source) return copyJson(encoded) as unknown as Checkpoint;
  const steps = valueSteps(store, source); for (;;) { const step = steps.next(); if (step.done) return checkpointWithSource(source, step.value); }
}
export async function readCheckpointValueAsync(store: SQLiteStore, actionID: string, encoded: Json, evidence = false, assertCurrent: () => void = () => {}): Promise<Checkpoint> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority(); assert(); const epoch = store.coordinatorEpoch;
  const source = sourceFor(store, actionID, encoded); if (!source) return copyJson(encoded) as unknown as Checkpoint;
  const steps = valueSteps(store, source);
  for (;;) { assert(); assertCurrent(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Checkpoint reader owner changed"); const step = steps.next(); if (step.done) return checkpointWithSource(source, step.value); await new Promise<void>(resolve => setImmediate(resolve)); }
}
export function expandCheckpointFields(store: SQLiteStore, input: Json): Json {
  if (!object(input) || typeof input.id !== "string" || !object(input.checkpoint)) return input;
  return { ...input, checkpoint: readCheckpointValue(store, input.id, input.checkpoint) as unknown as Json };
}
export async function expandCheckpointFieldsAsync(store: SQLiteStore, input: Json): Promise<Json> {
  if (!object(input) || typeof input.id !== "string" || !object(input.checkpoint)) return input;
  return { ...input, checkpoint: await readCheckpointValueAsync(store, input.id, input.checkpoint) as unknown as Json };
}
export async function readCheckpointHistory(store: SQLiteStore, actionID: string, encoded: unknown): Promise<Json> {
  const body = parseJson(encoded as string); if (!object(body) || body.actionID !== actionID || typeof body.attemptID !== "string" || !object(body.checkpoint)) invalid();
  const checkpoint = await readCheckpointValueAsync(store, actionID, body.checkpoint);
  return { ...body, checkpoint: checkpoint as unknown as Json };
}
