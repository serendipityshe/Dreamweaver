import { createHash } from "node:crypto";
import { InternalCanonicalJsonDecoder, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, validatePreparedActionRequest, type Json, type PreparedActionRequest } from "../contract/index.js";
import { ValueStore } from "./value-store.js";
import type { SQLiteStore, ValuePublication, SQLWrite } from "./sqlite.js";
import { policySourceSteps, storedPolicySource, policyMetadata, samePolicyValue, verifyPolicySource, type PolicySource } from "./policy-values.js";
type Row = Record<string, unknown>;
export interface ActionRequestValue { key: string; source: { encoding: "action-input-config-v1"; frameID: string; roundID: string; generation: number; value: ValuePublication } }
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid fixed Action request source"); }
const object = (v: unknown): v is Record<string, Json> => !!v && typeof v === "object" && !Array.isArray(v);
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
const same = (a: unknown, b: unknown) => {
  const x = canonicalJsonChunks(a)[Symbol.iterator](), y = canonicalJsonChunks(b)[Symbol.iterator]();
  for (;;) { const p = x.next(), q = y.next(); if (p.done || q.done) return p.done === q.done; if (!Buffer.from(p.value).equals(q.value)) return false; }
};

/** Prepare/seal before entering Core. The stable finalization identity allows
 * replay of an interrupted staging write without allocating another value. */
export async function stageActionRequestValues(store: SQLiteStore, prep: Row, assignmentID: string, requests: PreparedActionRequest[], quota: number,
  assertCurrent: () => void, guards: () => SQLWrite[]): Promise<ActionRequestValue[]> {
  const epoch = store.coordinatorEpoch, values = new ValueStore(store, quota), result: ActionRequestValue[] = [];
  const check = () => { store.assertAuthority(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Request staging owner changed"); assertCurrent(); };
  for (const request of requests) {
    const fields = { input: request.input, config: request.config }; let length = 0; const hash = createHash("sha256");
    for (const bytes of canonicalJsonChunks(fields)) { check(); length += bytes.length; hash.update(bytes); await new Promise<void>(r => setImmediate(r)); }
    if (!Number.isSafeInteger(length)) throw harnessError("LIMIT_EXCEEDED", "Action input length exceeds supported range");
    if (length <= 16_384) continue;
    const digest = hash.digest("hex"), frameID = prep.frame_id as string, roundID = prep.round_id as string, generation = prep.generation as number;
    const identity = canonicalJsonDigest([store.domainID, frameID, roundID, generation, assignmentID, request.key, digest]);
    const value: ValuePublication = { id: `action-input:${identity}`, owner: `action-input:${store.domainID}`, encoding: "utf8-v1", length, digest };
    check(); store.checkGuards(guards());
    values.begin(value, [...guards(), write("INSERT INTO action_request_staging(value_id,frame_id,round_id,generation,assignment_id,action_key) VALUES(?,?,?,?,?,?)", [value.id, frameID, roundID, generation, assignmentID, request.key])]);
    let offset = 0;
    for (const bytes of canonicalJsonChunks(fields)) {
      check(); values.append(value.id, value.owner, offset, bytes, guards()); offset += bytes.length;
      await new Promise<void>(r => setImmediate(r));
    }
    check(); await values.seal(value.id, value.owner); check(); store.checkGuards(guards());
    result.push({ key: request.key, source: { encoding: "action-input-config-v1", frameID, roundID, generation, value } });
  }
  return result;
}

export function encodeFixedActionRequest(request: PreparedActionRequest, values: ActionRequestValue[], policySource: PolicySource | null = null): Json {
  const source = values.find(value => value.key === request.key)?.source;
  const body = policySource ? { ...request, policies: policyMetadata(request.policies), policySource } : request;
  return source ? copyJson({ ...body, input: null, config: null, valueSource: source }) : copyJson(body);
}

export function requestValuePublicationWrites(values: ActionRequestValue[], assignmentID: string): SQLWrite[] {
  return values.map(({ key, source }) => write("DELETE FROM action_request_staging WHERE value_id=? AND frame_id=? AND round_id=? AND generation=? AND assignment_id=? AND action_key=?",
    [source.value.id, source.frameID, source.roundID, source.generation, assignmentID, key]));
}

/** These facts cannot become eligible again: terminal Run, ended frame/round,
 * changed preparation or finalization. Time alone is never retirement proof. */
const stale = `(r.business_terminal=1 OR f.state<>'open' OR p.round_id<>s.round_id OR p.generation<>s.generation OR p.state<>'ready'
  OR q.state<>'pending' OR j.assignment_id IS NOT s.assignment_id OR w.stage<>'finalize')`;
const owners = `action_request_staging s JOIN frames f ON f.id=s.frame_id JOIN runs r ON r.id=f.run_id
 JOIN dispatch_preparations p ON p.frame_id=f.id JOIN prepare_rounds q ON q.id=s.round_id
 JOIN finalize_jobs j ON j.id=f.id JOIN work_items w ON w.kind='run' AND w.subject_id=r.id`;
export async function retireActionRequestValues(store: SQLiteStore): Promise<void> {
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  if (store.query("SELECT s.value_id FROM action_request_staging s JOIN value_manifests m ON m.id=s.value_id WHERE m.state='deleting' LIMIT 1")[0]) await values.resumeUnboundRetirements();
  const rows = store.query(`SELECT m.*,s.frame_id,s.round_id,r.id run_id,r.business_terminal,f.state frame_state,
 p.round_id current_round,p.generation,p.state prep_state,q.state round_state,j.assignment_id,j.state job_state,w.id work_id,w.stage FROM ${owners} JOIN value_manifests m ON m.id=s.value_id
    WHERE m.state IN ('staging','sealed') AND ${stale} AND NOT EXISTS(SELECT 1 FROM value_references v WHERE v.value_id=m.id) LIMIT 16`);
  for (const row of rows) await values.retireUnbound({ id: row.id as string, owner: row.owner as string, encoding: row.encoding as string,
    length: row.total_bytes as number, digest: row.digest as string, written: row.written_bytes as number, revision: row.revision as number, state: row.state as "staging" | "sealed" },
    [write("UPDATE runs SET business_terminal=? WHERE id=? AND business_terminal=?", [row.business_terminal as number, row.run_id as string, row.business_terminal as number]),
     write("UPDATE frames SET state=? WHERE id=? AND state=?", [row.frame_state as string, row.frame_id as string, row.frame_state as string]),
     write("UPDATE dispatch_preparations SET state=? WHERE frame_id=? AND round_id=? AND generation=? AND state=?", [row.prep_state as string, row.frame_id as string, row.current_round as string, row.generation as number, row.prep_state as string]),
     write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=?", [row.round_state as string, row.round_id as string, row.round_state as string]),
     write("UPDATE finalize_jobs SET state=? WHERE id=? AND state=? AND assignment_id IS ?", [row.job_state as string, row.frame_id as string, row.job_state as string, row.assignment_id as string | null]),
     write("UPDATE work_items SET stage=? WHERE id=? AND stage=?", [row.stage as string, row.work_id as string, row.stage as string])]);
}

/** Authenticate the fixed publication without re-reading its business value. */
export function actionRequestValueSource(store: SQLiteStore, actionID: string, encoded?: unknown): ActionRequestValue["source"] | null {
  const action = store.query("SELECT request_json FROM actions WHERE id=?", [actionID])[0];
  if (!action || encoded !== undefined && action.request_json !== encoded) invalid();
  const body = parseJson(action.request_json as string); if (!object(body)) invalid();
  if (!Object.hasOwn(body, "valueSource")) return null;
  const source = body.valueSource as unknown as ActionRequestValue["source"], ref = source?.value;
  if (!source || source.encoding !== "action-input-config-v1" || typeof source.frameID !== "string" || typeof source.roundID !== "string" || !Number.isSafeInteger(source.generation)
      || body.input !== null || body.config !== null || !ref || ref.encoding !== "utf8-v1" || typeof ref.id !== "string" || typeof ref.owner !== "string"
      || !Number.isSafeInteger(ref.length) || ref.length <= 0 || !/^[a-f0-9]{64}$/.test(ref.digest)) invalid();
  const row = store.query(`SELECT m.owner,m.encoding,m.total_bytes,m.digest,m.state,m.written_bytes,h.operation_id,h.request_json
    FROM actions a JOIN value_manifests m ON m.id=? JOIN value_references v ON v.value_id=m.id
    JOIN history h ON h.operation_id=v.operation_id AND h.subject_id=a.run_id AND h.type='core.suspend'
    WHERE a.id=? AND a.request_json=?`, [ref.id, actionID, action.request_json as string])[0];
  if (!row || row.owner !== ref.owner || row.encoding !== ref.encoding || row.total_bytes !== ref.length || row.written_bytes !== ref.length || row.digest !== ref.digest || row.state !== "published") invalid();
  const receipt = parseJson(row.request_json as string);
  if (!object(receipt) || receipt.frameID !== source.frameID || receipt.roundID !== source.roundID || receipt.generation !== source.generation
      || !Array.isArray(receipt.actionRequestValues) || receipt.actionRequestValues.filter(value => object(value) && value.key === body.key && same(value.source, source)).length !== 1) invalid();
  return source;
}

/** Report-output validation reuses the already committed fixed input/config. */
export function compactActionRequestPayload(store: SQLiteStore, payload: unknown, existing?: unknown): Json {
  const value = copyJson(payload); if (!object(value) || value.phase !== "report-output" || !object(value.context) || !object(value.context.prepared)) return value;
  if (existing !== undefined) { const previous = typeof existing === "string" ? parseJson(existing) : existing; if (!object(previous) || !object(previous.context) || previous.context.actionRequestSource === undefined) return value; }
  const context = value.context, prepared = context.prepared as Record<string, Json>;
  if (typeof context.actionID !== "string") invalid();
  const source = actionRequestValueSource(store, context.actionID); if (!source) return value;
  if (!same(value.config, prepared.config) || canonicalJsonDigest({ input: prepared.input, config: prepared.config }) !== source.value.digest) invalid();
  context.actionRequestSource = copyJson({ actionID: context.actionID, source });
  prepared.input = null; prepared.config = null; value.config = null; return value;
}

/** Match an output-validation payload after its valueSlots aliases are expanded.
 * New jobs compare authenticated source bindings and small metadata; legacy jobs
 * without source markers still compare their original native values in chunks. */
export function fixedActionValidationMatches(store: SQLiteStore, actionID: string, payload: unknown, encoded: unknown): boolean {
  if (!object(payload) || payload.phase !== "report-output" || !object(payload.context) || !object(payload.context.prepared)) return false;
  const context = payload.context, prepared = context.prepared as Record<string, Json>;
  if (context.actionID !== actionID) return false;
  const stored = parseJson(encoded as string); if (!object(stored)) invalid();
  const source = actionRequestValueSource(store, actionID, encoded), policySource = storedPolicySource(stored);
  const referencedInput = Object.hasOwn(context, "actionRequestSource"), referencedPolicies = Object.hasOwn(context, "policySource");
  if (referencedInput && (!source || !same(context.actionRequestSource, { actionID, source })
      || prepared.input !== null || prepared.config !== null || payload.config !== null)) return false;
  if (referencedPolicies) {
    if (!policySource || !same(context.policySource, policySource)) return false;
    verifyPolicySource(store, policySource, stored.policies);
  }
  const { valueSource: _valueSource, policySource: _policySource, ...metadata } = stored;
  // Only old pending payloads need materialization. Never compare a placeholder
  // to a native value, or treat an unverified caller-supplied digest as evidence.
  const expected = (source && !referencedInput || policySource && !referencedPolicies)
    ? copyJson(readFixedActionRequest(store, actionID, encoded)) as Record<string, Json> : metadata;
  if (referencedInput) { expected.input = null; expected.config = null; }
  if (referencedPolicies) expected.policies = stored.policies!;
  return same(expected, prepared) && same(context.policies, expected.policies) && same(payload.config, expected.config);
}

function* readSteps(store: SQLiteStore, actionID: string, encoded: unknown): Generator<void, PreparedActionRequest> {
  const stored = parseJson(encoded as string); if (!object(stored)) invalid();
  const policySource = storedPolicySource(stored), { policySource: _policySource, ...body } = stored;
  if (policySource) {
    const policies = yield* policySourceSteps(store, policySource);
    if (!samePolicyValue(body.policies, policyMetadata(policies))) invalid(); body.policies = copyJson(policies);
  }
  if (!Object.hasOwn(body, "valueSource")) return validatePreparedActionRequest(body);
  const source = actionRequestValueSource(store, actionID, encoded); if (!source) invalid(); const ref = source.value;
  const hash = createHash("sha256"), decoder = new InternalCanonicalJsonDecoder(); let offset = 0;
  while (offset < ref.length) {
    const chunk = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [ref.id, offset])[0];
    if (!chunk || !(chunk.payload_blob instanceof Uint8Array) || chunk.payload_blob.length !== chunk.length_bytes || !chunk.payload_blob.length
        || chunk.payload_blob.length > 65_536 || chunk.payload_blob.length > ref.length - offset || createHash("sha256").update(chunk.payload_blob).digest("hex") !== chunk.chunk_digest) invalid();
    decoder.write(chunk.payload_blob); hash.update(chunk.payload_blob); offset += chunk.payload_blob.length; yield;
  }
  if (hash.digest("hex") !== ref.digest) invalid();
  const fields = decoder.finish(); if (!object(fields) || Object.keys(fields).sort().join(",") !== "config,input") invalid();
  const { valueSource: _source, ...request } = body;
  return validatePreparedActionRequest({ ...request, input: fields.input, config: fields.config });
}

/** Sync evidence/startup boundary; callers retain their authority guards. */
export function readFixedActionRequest(store: SQLiteStore, actionID: string, encoded: unknown): PreparedActionRequest {
  const steps = readSteps(store, actionID, encoded); for (;;) { const step = steps.next(); if (step.done) return step.value; }
}
export async function readFixedActionRequestAsync(store: SQLiteStore, actionID: string, encoded: unknown, evidence = false): Promise<PreparedActionRequest> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assert(); const epoch = store.coordinatorEpoch, steps = readSteps(store, actionID, encoded);
  for (;;) {
    assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Request reader owner changed");
    const step = steps.next(); if (step.done) return step.value;
    await new Promise<void>(r => setImmediate(r));
  }
}
export function fixedActionRequestMatches(store: SQLiteStore, actionID: string, native: unknown, encoded: unknown): boolean {
  return same(native, readFixedActionRequest(store, actionID, encoded));
}
