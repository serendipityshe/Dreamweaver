import { createHash, randomUUID } from "node:crypto";
import { InternalCanonicalJsonDecoder, canonicalJson, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, validateReport, type Json, type ResolveActionRequest, type ActionReport } from "../contract/index.js";
import { actionReportValueReference, readActionReport, readActionReportForEvidence, readActionReportSync } from "./report-value.js";
import { ValueStore } from "./value-store.js";
import type { SQLiteStore, SQLWrite, ValuePublication } from "./sqlite.js";

type Obj = Record<string, Json>;
const object = (value: unknown): value is Obj => !!value && typeof value === "object" && !Array.isArray(value);
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid durable command value source"); }
export interface CommandValueSource { encoding: "command-value-v1"; principalID: string; requestKey: string; actionID: string; value: ValuePublication }
export interface CommandEvidenceSource { encoding: "command-evidence-v1"; principalID: string; requestKey: string; operation: "resumeRun" | "adoptVersion"; subjectID: string; operationID: string; value: ValuePublication }
export type CommandSelector = "request" | "evidence" | "result" | "measurements" | "report";
export interface CommandBinding { path: (string | number)[]; selector: CommandSelector }

/** The same bounded value store and epoch-owned staging lifecycle used by
 * submit; staging alone never reserves a command key or proves acceptance. */
export async function stageEpochValue(store: SQLiteStore, rawValue: unknown, refOwner: string, idPrefix: string, quota: number, assertCaller: () => void, force = false): Promise<ValuePublication | null> {
  const epoch = store.coordinatorEpoch, values = new ValueStore(store, quota);
  const check = () => { store.assertAuthority(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Epoch value staging owner changed"); assertCaller(); };
  check(); const chunks = canonicalJsonChunks(rawValue);
  let length = 0; const hash = createHash("sha256");
  for (const chunk of chunks) { check(); length += chunk.length; hash.update(chunk); await new Promise<void>(r => setImmediate(r)); }
  if (!Number.isSafeInteger(length)) throw harnessError("LIMIT_EXCEEDED", "Epoch value exceeds supported length");
  if (length <= 16_384 && !force) return null;
  const value: ValuePublication = { id: `${idPrefix}:${randomUUID()}`, owner: refOwner, encoding: "utf8-v1", length, digest: hash.digest("hex") };
  const guards = () => [write("UPDATE command_value_staging SET state='active' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [value.id, epoch])];
  try {
    check(); values.begin(value, [write("INSERT INTO command_value_staging(value_id,coordinator_epoch,state) VALUES(?,?,?)", [value.id, epoch, "active"])]);
    let offset = 0;
    for (const chunk of chunks) { check(); values.append(value.id, value.owner, offset, chunk, guards()); offset += chunk.length; await new Promise<void>(r => setImmediate(r)); }
    check(); await values.seal(value.id, value.owner); check(); store.checkGuards(guards()); return value;
  } catch (error) { abandonEpochValue(store, value); throw error; }
}
export async function stageCommandValue(store: SQLiteStore, principalID: string, request: ResolveActionRequest, quota: number, assertCaller: () => void, force = false): Promise<CommandValueSource | null> {
  const value = await stageEpochValue(store, request, `command-value:${store.domainID}`, "command-value", quota, assertCaller, force);
  return value ? { encoding: "command-value-v1", principalID, requestKey: request.requestKey, actionID: request.actionID, value } : null;
}
export function abandonEpochValue(store: SQLiteStore, value: ValuePublication | null): void {
  if (!value || store.requiresRecovery) return;
  try {
    store.assertAuthority();
    const row = store.query("SELECT coordinator_epoch,state FROM command_value_staging WHERE value_id=?", [value.id])[0];
    if (!row || row.state !== "active" || row.coordinator_epoch !== store.coordinatorEpoch) return;
    store.commit({ operationID: `command-value.abandon:${value.id}`, kind: "command_value.abandoned", subjectID: value.id, request: { valueID: value.id }, evidence: null, result: null,
      writes: [write("UPDATE command_value_staging SET state='abandoned' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [value.id, row.coordinator_epoch as number])] });
  } catch { /* Fenced/unknown storage is decided by recovery. */ }
}
export function abandonCommandValue(store: SQLiteStore, source: CommandValueSource | CommandEvidenceSource | null): void { abandonEpochValue(store, source?.value ?? null); }
/** Optional shared epoch ownership; older/v4 staged sources use their own
 * immutable ownership evidence and have no row in this registry. */
export function epochValuePublicationWrites(store: SQLiteStore, value: ValuePublication, epoch: number): SQLWrite[] {
  const row = store.query("SELECT coordinator_epoch,state FROM command_value_staging WHERE value_id=?", [value.id])[0];
  if (!row) return [];
  if (row.coordinator_epoch !== epoch || row.state !== "active") throw harnessError("EXECUTION_FENCED", "Epoch staged value is no longer publishable");
  return [write("DELETE FROM command_value_staging WHERE value_id=? AND coordinator_epoch=? AND state='active'", [value.id, epoch])];
}
export function commandPublicationWrites(source: CommandValueSource | CommandEvidenceSource | null, epoch: number): SQLWrite[] {
  return source ? [write("DELETE FROM command_value_staging WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, epoch])] : [];
}
export async function retireCommandValues(store: SQLiteStore): Promise<void> {
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  if (store.query("SELECT s.value_id FROM command_value_staging s JOIN value_manifests m ON m.id=s.value_id WHERE m.state='deleting' LIMIT 1")[0]) await values.resumeUnboundRetirements();
  const rows = store.query(`SELECT m.*,s.coordinator_epoch,s.state staging_state FROM command_value_staging s JOIN value_manifests m ON m.id=s.value_id
    WHERE m.state IN ('staging','sealed') AND (s.state='abandoned' OR s.coordinator_epoch<?) AND NOT EXISTS(SELECT 1 FROM value_references v WHERE v.value_id=m.id) LIMIT 16`, [store.coordinatorEpoch]);
  for (const row of rows) await values.retireUnbound({ id: row.id as string, owner: row.owner as string, encoding: row.encoding as string, length: row.total_bytes as number,
    digest: row.digest as string, written: row.written_bytes as number, revision: row.revision as number, state: row.state as "staging" | "sealed" },
    [write("UPDATE command_value_staging SET state=? WHERE value_id=? AND coordinator_epoch=? AND state=?", [row.staging_state as string, row.id as string, row.coordinator_epoch as number, row.staging_state as string])]);
}
export function encodeCommandRequest(request: ResolveActionRequest, source: CommandValueSource | null): Json {
  if (!source) return copyJson(request);
  const resolution = request.resolution.kind === "settle" ? { ...request.resolution, result: request.resolution.result.kind === "succeeded" ? { kind: "succeeded", value: null } : { kind: "failed", error: { code: "SOURCE", message: "Referenced result", details: null } }, measurements: [] } : request.resolution;
  return copyJson({ ...request, evidence: null, resolution, commandSource: source });
}
export function commandRequestMetadata(encoded: unknown): ResolveActionRequest {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded); if (!object(body)) invalid();
  const { commandSource: source, ...metadata } = body;
  if (source !== undefined && (!object(source) || source.encoding !== "command-value-v1" || source.actionID !== body.actionID || source.requestKey !== body.requestKey || body.evidence !== null)) invalid();
  // Only validate/authorize the command envelope here. This finite shape
  // sentinel is never a returned evidence value or an execution permission.
  return (source === undefined ? metadata : { ...metadata, evidence: { source: "persisted-command" } }) as unknown as ResolveActionRequest;
}
export function verifyCommandValue(store: SQLiteStore, source: CommandValueSource): void {
  const ref = source?.value;
  if (!source || Object.keys(source).sort().join(",") !== "actionID,encoding,principalID,requestKey,value" || source.encoding !== "command-value-v1" || typeof source.principalID !== "string" || typeof source.requestKey !== "string" || typeof source.actionID !== "string"
      || !ref || Object.keys(ref).sort().join(",") !== "digest,encoding,id,length,owner" || typeof ref.id !== "string" || ref.owner !== `command-value:${store.domainID}` || ref.encoding !== "utf8-v1" || !Number.isSafeInteger(ref.length) || ref.length <= 0 || !/^[a-f0-9]{64}$/.test(ref.digest)) invalid();
  const operationID = `resolve:${canonicalJson([source.principalID, source.requestKey])}:register`;
  const proof = store.query(`SELECT c.request_json,m.owner,m.encoding,m.total_bytes,m.written_bytes,m.digest,m.state,h.request_json receipt_request,h.subject_id,h.type
    FROM commands c JOIN value_manifests m ON m.id=? JOIN value_references v ON v.value_id=m.id AND v.operation_id=? JOIN history h ON h.operation_id=v.operation_id
    WHERE c.principal_id=? AND c.request_key=? AND c.operation='resolveAction'`, [ref.id, operationID, source.principalID, source.requestKey])[0];
  if (!proof || proof.owner !== ref.owner || proof.encoding !== ref.encoding || proof.total_bytes !== ref.length || proof.written_bytes !== ref.length || proof.digest !== ref.digest || proof.state !== "published" || proof.subject_id !== source.actionID || !["action.resolve_registered", "action.retry_authorized"].includes(proof.type as string)
      || proof.receipt_request !== proof.request_json) invalid();
  const request = parseJson(proof.request_json as string) as Obj;
  if (canonicalJson(request.commandSource) !== canonicalJson(source) || request.actionID !== source.actionID || request.requestKey !== source.requestKey) invalid();
}
function* sourceSteps(store: SQLiteStore, source: CommandValueSource): Generator<void, ResolveActionRequest> {
  verifyCommandValue(store, source);
  const request = (yield* valueSteps(store, source.value)) as unknown as ResolveActionRequest;
  if (request.actionID !== source.actionID || request.requestKey !== source.requestKey) invalid(); return request;
}
function* valueSteps(store: SQLiteStore, value: ValuePublication): Generator<void, Json> {
  const hash = createHash("sha256"), decoder = new InternalCanonicalJsonDecoder(); let offset = 0;
  while (offset < value.length) {
    const row = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [value.id, offset])[0];
    if (!row || !(row.payload_blob instanceof Uint8Array) || row.payload_blob.length !== row.length_bytes || !row.payload_blob.length || row.payload_blob.length > 65_536 || row.payload_blob.length > value.length - offset || createHash("sha256").update(row.payload_blob).digest("hex") !== row.chunk_digest) invalid();
    decoder.write(row.payload_blob); hash.update(row.payload_blob); offset += row.payload_blob.length; yield;
  }
  if (hash.digest("hex") !== value.digest) invalid(); return decoder.finish();
}
export function readCommandRequest(store: SQLiteStore, encoded: unknown): ResolveActionRequest {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded); if (!object(body)) invalid();
  if (!body.commandSource) return body as unknown as ResolveActionRequest;
  const steps = sourceSteps(store, body.commandSource as unknown as CommandValueSource); for (;;) { const step = steps.next(); if (step.done) return step.value; }
}
export async function readCommandSource(store: SQLiteStore, source: CommandValueSource, evidence = false): Promise<ResolveActionRequest> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority(); assert(); const epoch = store.coordinatorEpoch, steps = sourceSteps(store, source);
  for (;;) { assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Command source owner changed"); const step = steps.next(); if (step.done) return step.value; await new Promise<void>(r => setImmediate(r)); }
}
export function commandSource(encoded: unknown): CommandValueSource | null { const body = typeof encoded === "string" ? parseJson(encoded) : encoded; return object(body) && body.commandSource ? body.commandSource as unknown as CommandValueSource : null; }
/** Startup inventory only; no business value is reconstructed before claiming
 * authority. The subsequent fenced pass verifies and decodes every source. */
export function commandProjectionByteBound(store: SQLiteStore, encoded: unknown): number {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded);
  let bytes = 0; for (const chunk of canonicalJsonChunks(body)) bytes += chunk.length;
  if (!object(body)) return bytes;
  if (body.encoding === "command-evidence-projection-v1") {
    const source = body.source as unknown as CommandEvidenceSource; verifyCommandEvidence(store, source);
    if (!Array.isArray(body.paths)) invalid(); bytes += body.paths.length * source.value.length;
  } else if (body.encoding === "command-projection-v1") {
    const source = body.source as unknown as CommandValueSource; verifyCommandValue(store, source); verifyProjectionCommit(store, source);
    if (!Array.isArray(body.bindings)) invalid(); let length = source.value.length;
    const row = store.query("SELECT request_json FROM commands WHERE principal_id=? AND request_key=? AND operation='resolveAction'", [source.principalID, source.requestKey])[0];
    const request = commandRequestMetadata(row?.request_json), report = legacyReportRow(store, request);
    if (report) length += actionReportValueReference(store, report)?.length ?? Buffer.byteLength(report.report_json as string, "utf8");
    bytes += body.bindings.length * length;
  }
  if (!Number.isSafeInteger(bytes)) invalid(); return bytes;
}

/** Private root wrapper: business objects with a similarly named field are
 * untouched. Each slot is restored from the command's original committed bytes. */
export function commandProjection(source: CommandValueSource | null, template: unknown, bindings: CommandBinding[]): Json {
  if (!source) return copyJson(template);
  let body = template as Json;
  for (const binding of bindings) body = mask(body, binding.path);
  return copyJson({ encoding: "command-projection-v1", source, template: body, bindings, ...(object(body) && typeof body.kind === "string" ? { kind: body.kind } : {}) });
}
/** Copy only the short path to a slot before removing it. In particular, do
 * not clone the business subtree that the committed source already retains. */
function mask(root: Json, path: (string | number)[]): Json {
  if (!path.length) return null;
  if (!root || typeof root !== "object" || !Object.hasOwn(root, path[0]!)) invalid();
  const value: Json[] | Obj = Array.isArray(root) ? root.slice() : { ...root };
  Object.defineProperty(value, path[0]!, { value: mask((root as Obj)[path[0]!]!, path.slice(1)), enumerable: true, writable: true, configurable: true }); return value;
}
function replace(root: Json, path: (string | number)[], value: Json): Json {
  if (!path.length) return value;
  let parent = root;
  for (const key of path.slice(0, -1)) { if (!parent || typeof parent !== "object" || !Object.hasOwn(parent, key)) invalid(); parent = (parent as Obj)[key]!; }
  if (!parent || typeof parent !== "object" || !Object.hasOwn(parent, path.at(-1)!)) invalid();
  Object.defineProperty(parent, path.at(-1)!, { value, enumerable: true, writable: true, configurable: true }); return root;
}
function legacyReportRow(store: SQLiteStore, request: ResolveActionRequest): Record<string, unknown> | null {
  if (request.resolution.kind !== "legacy_report") return null;
  const resolution = request.resolution;
  const row = store.query("SELECT * FROM action_reports WHERE id=? AND action_id=? AND attempt_id=? AND phase=?", [resolution.reportID, request.actionID, resolution.attemptID, resolution.phase])[0];
  if (!row) invalid(); return row;
}
function expand(body: Obj, request: ResolveActionRequest, legacy?: ActionReport): Json {
  if (!Array.isArray(body.bindings) || !body.bindings.length) invalid();
  let output = copyJson(body.template);
  const resolution = request.resolution.kind === "settle" ? request.resolution : null;
  let report: Json | null = resolution ? copyJson({ payload: resolution.result, evidence: request.evidence, measurements: resolution.measurements }) : null;
  let result: Json | null = resolution ? copyJson(resolution.result) : null, reportEvidence = request.evidence, measurements: Json | null = resolution ? copyJson(resolution.measurements) : null;
  if (request.resolution.kind === "legacy_report") {
    if (!legacy || legacy.kind !== "report" || canonicalJsonDigest(legacy) !== request.resolution.reportDigest) invalid();
    const parsed = request.resolution.phase === "execute" ? validateReport(legacy.value, "execute") : validateReport(legacy.value, "reconcile");
    const raw = request.resolution.phase === "execute" ? parsed.payload : parsed.payload.kind === "settled" ? parsed.payload.result : null;
    if (!raw || raw.kind !== "succeeded" && raw.kind !== "failed") invalid();
    result = copyJson(raw); reportEvidence = parsed.evidence; measurements = copyJson(parsed.measurements); report = legacy.value;
  }
  for (const raw of body.bindings) {
    if (!object(raw) || !Array.isArray(raw.path) || raw.path.some(p => typeof p !== "string" && (!Number.isSafeInteger(p) || (p as number) < 0))) invalid();
    const selected = raw.selector === "request" ? request : raw.selector === "evidence" ? reportEvidence : raw.selector === "result" && result ? result : raw.selector === "measurements" && measurements ? measurements : raw.selector === "report" && report ? report : invalid();
    output = replace(output, raw.path as (string | number)[], copyJson(selected));
  }
  return output;
}
function verifyProjectionCommit(store: SQLiteStore, source: CommandValueSource): void {
  const id = `resolve:${canonicalJson([source.principalID, source.requestKey])}`;
  const row = store.query(`SELECT c.status FROM commands c JOIN history h ON h.request_json=c.request_json AND h.subject_id=?
    WHERE c.principal_id=? AND c.request_key=? AND c.operation='resolveAction' AND c.status='applied'
      AND ((h.operation_id=? AND h.type='action.resolve_applied') OR (h.operation_id=? AND h.type='action.retry_authorized'))`, [source.actionID, source.principalID, source.requestKey, `${id}:apply`, `${id}:register`])[0];
  if (!row) invalid();
}
export function readCommandProjectionSync(store: SQLiteStore, encoded: unknown, actionID?: string): Json {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded);
  if (object(body) && body.encoding === "command-evidence-projection-v1") return readCommandEvidenceProjectionSync(store, body);
  if (!object(body) || body.encoding !== "command-projection-v1") return body;
  const source = body.source as unknown as CommandValueSource; verifyCommandValue(store, source);
  verifyProjectionCommit(store, source);
  if (actionID !== undefined && source.actionID !== actionID) invalid();
  const request = readCommandRequest(store, { commandSource: source }), legacyRow = legacyReportRow(store, request);
  return expand(body, request, legacyRow ? readActionReportSync(store, legacyRow) : undefined);
}
export async function readCommandProjection(store: SQLiteStore, encoded: unknown, evidence = false, actionID?: string): Promise<Json> {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded);
  if (object(body) && body.encoding === "command-evidence-projection-v1") return readCommandEvidenceProjection(store, body, evidence);
  if (!object(body) || body.encoding !== "command-projection-v1") return body;
  verifyProjectionCommit(store, body.source as unknown as CommandValueSource);
  if (actionID !== undefined && (body.source as unknown as CommandValueSource).actionID !== actionID) invalid();
  const request = await readCommandSource(store, body.source as unknown as CommandValueSource, evidence), legacyRow = legacyReportRow(store, request);
  const legacy = legacyRow ? await (evidence ? readActionReportForEvidence(store, legacyRow) : readActionReport(store, legacyRow)) : undefined;
  return expand(body, request, legacy);
}
export const commandValueMigration = `
CREATE TABLE command_value_staging(value_id TEXT PRIMARY KEY REFERENCES value_manifests(id) ON DELETE CASCADE,coordinator_epoch INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN ('active','abandoned'))) STRICT;
CREATE TRIGGER command_value_staging_epoch BEFORE INSERT ON command_value_staging WHEN NEW.coordinator_epoch<>(SELECT coordinator_epoch FROM domain WHERE singleton=1) BEGIN SELECT RAISE(ABORT,'stale command staging epoch'); END;
`;

export async function stageCommandEvidence(store: SQLiteStore, principalID: string, operation: CommandEvidenceSource["operation"], request: { requestKey: string; evidence: Json }, subjectID: string, operationID: string, quota: number, assertCaller: () => void): Promise<CommandEvidenceSource | null> {
  const staged = await stageCommandValue(store, principalID, { requestKey: request.requestKey, actionID: subjectID, expectedRevision: 0, resolution: { kind: "allow_retry" }, evidence: request.evidence }, quota, assertCaller);
  return staged ? { encoding: "command-evidence-v1", principalID, requestKey: request.requestKey, operation, subjectID, operationID, value: staged.value } : null;
}
export function encodeCommandEvidence(request: { requestKey: string; evidence: Json }, source: CommandEvidenceSource | null): Json {
  return source ? copyJson({ ...request, evidence: null, evidenceSource: source }) : copyJson(request);
}
export function commandEvidenceMetadata(encoded: unknown): Json {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded); if (!object(body)) invalid();
  const { evidenceSource, ...metadata } = body;
  if (evidenceSource !== undefined && (!object(evidenceSource) || evidenceSource.encoding !== "command-evidence-v1" || evidenceSource.requestKey !== body.requestKey || body.evidence !== null)) invalid();
  return evidenceSource === undefined ? metadata : { ...metadata, evidence: { source: "persisted-command" } };
}
export function verifyCommandEvidence(store: SQLiteStore, source: CommandEvidenceSource): void {
  const ref = source?.value;
  if (!source || Object.keys(source).sort().join(",") !== "encoding,operation,operationID,principalID,requestKey,subjectID,value" || source.encoding !== "command-evidence-v1" || !["resumeRun", "adoptVersion"].includes(source.operation)
      || typeof source.principalID !== "string" || typeof source.requestKey !== "string" || typeof source.subjectID !== "string" || typeof source.operationID !== "string"
      || !ref || Object.keys(ref).sort().join(",") !== "digest,encoding,id,length,owner" || typeof ref.id !== "string" || ref.owner !== `command-value:${store.domainID}` || ref.encoding !== "utf8-v1" || !Number.isSafeInteger(ref.length) || ref.length <= 0 || !/^[a-f0-9]{64}$/.test(ref.digest)) invalid();
  const proof = store.query(`SELECT c.request_json,m.owner,m.encoding,m.total_bytes,m.written_bytes,m.digest,m.state,h.request_json receipt_request,h.subject_id
    FROM commands c JOIN value_manifests m ON m.id=? JOIN value_references v ON v.value_id=m.id AND v.operation_id=? JOIN history h ON h.operation_id=v.operation_id
    WHERE c.principal_id=? AND c.request_key=? AND c.operation=?`, [ref.id, source.operationID, source.principalID, source.requestKey, source.operation])[0];
  if (!proof || proof.owner !== ref.owner || proof.encoding !== ref.encoding || proof.total_bytes !== ref.length || proof.written_bytes !== ref.length || proof.digest !== ref.digest || proof.state !== "published" || proof.subject_id !== source.subjectID || proof.receipt_request !== proof.request_json) invalid();
  const request = parseJson(proof.request_json as string) as Obj;
  if (canonicalJson(request.evidenceSource) !== canonicalJson(source) || request.requestKey !== source.requestKey) invalid();
}
export function readCommandEvidence(store: SQLiteStore, source: CommandEvidenceSource): Json {
  verifyCommandEvidence(store, source); const steps = valueSteps(store, source.value);
  for (;;) { const step = steps.next(); if (step.done) { if (!object(step.value) || step.value.requestKey !== source.requestKey || step.value.actionID !== source.subjectID || !Object.hasOwn(step.value, "evidence")) invalid(); return step.value.evidence!; } }
}
export async function readCommandEvidenceAsync(store: SQLiteStore, source: CommandEvidenceSource, recoveryEvidence = false): Promise<Json> {
  const assert = () => recoveryEvidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assert(); const epoch = store.coordinatorEpoch; verifyCommandEvidence(store, source); const steps = valueSteps(store, source.value);
  for (;;) {
    assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Command evidence owner changed");
    const step = steps.next();
    if (step.done) { verifyCommandEvidence(store, source); if (!object(step.value) || step.value.requestKey !== source.requestKey || step.value.actionID !== source.subjectID || !Object.hasOwn(step.value, "evidence")) invalid(); return step.value.evidence!; }
    await new Promise<void>(r => setImmediate(r));
  }
}
export function readCommandEvidenceRequest<T>(store: SQLiteStore, encoded: unknown): T {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded); if (!object(body)) invalid();
  const metadata = commandEvidenceMetadata(body) as Obj;
  return (body.evidenceSource ? { ...metadata, evidence: readCommandEvidence(store, body.evidenceSource as unknown as CommandEvidenceSource) } : metadata) as T;
}
export function commandEvidenceProjection(source: CommandEvidenceSource | null, template: unknown, paths: (string | number)[][]): Json {
  if (!source) return copyJson(template); let body = template as Json; for (const path of paths) body = mask(body, path);
  return copyJson({ encoding: "command-evidence-projection-v1", source, template: body, paths });
}
export function readCommandEvidenceProjectionSync(store: SQLiteStore, encoded: unknown): Json {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded);
  if (!object(body) || body.encoding !== "command-evidence-projection-v1") return body;
  return expandEvidenceProjection(body, readCommandEvidence(store, body.source as unknown as CommandEvidenceSource));
}
export async function readCommandEvidenceProjection(store: SQLiteStore, encoded: unknown, recoveryEvidence = false): Promise<Json> {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded);
  if (!object(body) || body.encoding !== "command-evidence-projection-v1") return body;
  return expandEvidenceProjection(body, await readCommandEvidenceAsync(store, body.source as unknown as CommandEvidenceSource, recoveryEvidence));
}
function expandEvidenceProjection(body: Obj, evidence: Json): Json {
  if (!Array.isArray(body.paths) || !body.paths.length) invalid();
  let output = copyJson(body.template);
  for (const path of body.paths) { if (!Array.isArray(path) || path.some(p => typeof p !== "string" && (!Number.isSafeInteger(p) || (p as number) < 0))) invalid(); output = replace(output, path as (string | number)[], evidence); } return output;
}
