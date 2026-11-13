import { readRunFrame, encodeRunInput, readRunInput } from "./run-input.js";
import { createHash } from "node:crypto";
import { canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, type Json } from "../contract/index.js";
import { readFrameDecision } from "./driver-value.js";
import { readCompletionRequest } from "./completion-request.js";
import { readPreparationOriginals, readPreparationResolved } from "./preparation-values.js";
import type { SQLiteStore } from "./sqlite.js";
import { continuationSource } from "./continuation.js";
import { readValidationConclusion } from "./validation-value.js";
import { initialWaitSource, readInitialWaitWake } from "./initial-wake.js";

type ObjectJson = { [key: string]: Json };
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid dispatch request source"); }
function object(value: unknown): ObjectJson {
  if (!value || typeof value !== "object" || Array.isArray(value)) invalid();
  return value as ObjectJson;
}
function objects(value: unknown): ObjectJson[] {
  if (!Array.isArray(value)) invalid();
  return (value as unknown[]).map(object);
}
function witness(value: unknown): Json {
  const hash = createHash("sha256"); let length = 0;
  for (const part of canonicalJsonChunks(value)) { length += part.length; hash.update(part); }
  if (!Number.isSafeInteger(length)) invalid();
  return { length, digest: hash.digest("hex") };
}
const same = (a: unknown, b: unknown) => canonicalJsonDigest(a) === canonicalJsonDigest(b);

function sources(store: SQLiteStore, runID: string, request: ObjectJson) {
  if (typeof request.frameID !== "string" || typeof request.roundID !== "string" || !Number.isSafeInteger(request.generation)) invalid();
  const row = store.query(`SELECT c.decision_json,p.* FROM frame_candidates c
    JOIN frames f ON f.id=c.frame_id JOIN dispatch_preparations p ON p.frame_id=c.frame_id
    JOIN prepare_rounds r ON r.id=p.round_id
    WHERE c.frame_id=? AND f.run_id=? AND p.round_id=? AND p.generation=? AND p.state='ready'
      AND r.owner_kind='frame' AND r.owner_id=c.frame_id AND r.phase='dispatch' AND r.state IN ('pending','accepted')`,
    [request.frameID, runID, request.roundID, request.generation as number])[0];
  if (!row || row.resolved_json === null) invalid();
  const decision = copyJson(readFrameDecision(store, request.frameID, row.decision_json));
  if (object(decision).kind !== "dispatch") invalid();
  const original = readPreparationOriginals(store, row), resolved = readPreparationResolved(store, row);
  const evidence = objects(request.admissionEvidence);
  if (original.length !== resolved.length || evidence.length !== resolved.length || new Set(evidence.map(entry => entry.id)).size !== evidence.length) invalid();
  const admission = evidence.map(entry => {
    if (typeof entry.id !== "string" || typeof entry.assignmentID !== "string" || !Number.isSafeInteger(entry.assignmentEpoch)) invalid();
    const job = store.query(`SELECT payload_json,report_json FROM validation_jobs WHERE id=? AND round_id=?
      AND state='accepted' AND assignment_id=? AND assignment_epoch=?`, [entry.id, request.roundID as string, entry.assignmentID, entry.assignmentEpoch as number])[0];
    if (!job || job.report_json === null) invalid();
    const payload = parseJson(job.payload_json as string), conclusion = copyJson(readValidationConclusion(store, job.report_json));
    if (object(payload).phase !== "admission" || object(conclusion).valid !== true) invalid();
    return { payload, conclusion };
  });
  return { decision, original, resolved, admission };
}

function identities(source: ReturnType<typeof sources>, actions: Json, children: Json): Json {
  return { decision: witness(source.decision), original: witness(source.original), resolved: witness(source.resolved),
    admission: source.admission.map(item => ({ payload: witness(item.payload), conclusion: witness(item.conclusion) })),
    actions: witness(actions), children: witness(children) };
}

/** Reuse accepted preparation sources. Action values, when present, were
 * sealed before this handoff and are published by its atomic commit. */
export function dispatchRequest(store: SQLiteStore, runID: string, input: unknown): Json {
  const request = object(copyJson(input));
  if (request.rootAfter) {
    const root = object(request.rootAfter), decision = object(request.decision), proposal = object(request.proposal);
    request.rootAfter = encodeRunInput(store, root, undefined, root.id === runID && decision.kind === "dispatch"
      ? continuationSource(runID, request.frameID as string, proposal.operationID as string, decision.continuation) : undefined);
  }
  let bytes = 0;
  for (const part of canonicalJsonChunks(request)) { bytes += part.length; if (bytes > 16_384) break; }
  if (bytes <= 16_384) return request;
  const source = sources(store, runID, request);
  if (!same(request.decision, source.decision) || !same(request.resolved, source.resolved)) invalid();
  const originalActions = objects(request.actions), originalChildren = objects(request.children), evidence = objects(request.admissionEvidence);
  if (originalActions.length !== source.resolved.length) invalid();
  const actions = originalActions.map((action, index) => {
    if (!same(action.original, source.original[index]) || !same(action.request, source.resolved[index])) invalid();
    return { ...action, original: null, request: null };
  });
  const children = originalChildren.map(child => {
    const parent = originalActions.find(action => action.remoteRunID === child.id && action.id === child.parentActionID);
    if (!parent || !same(child.input, object(parent.request).input) || !same(child.config, object(parent.request).config)) invalid();
    return { ...child, input: null, config: null };
  });
  const admissionEvidence = evidence.map((entry, index) => {
    if (!same(entry.payload, source.admission[index]!.payload) || !same(entry.conclusion, source.admission[index]!.conclusion)) invalid();
    return { ...entry, payload: null, conclusion: null };
  });
  return { ...request, encoding: "core-dispatch-ref-v1", decision: null, resolved: null, actions, children, admissionEvidence,
    sources: identities(source, request.actions!, request.children!) };
}

/** Materialize the original request before the existing recovery verifier
 * checks Core, leases, budgets, cancellation, Actions and child Run records. */
export function readTransitionRequest<T>(store: SQLiteStore, runID: string, encoded: unknown): T {
  const request = object(parseJson(encoded as string));
  if (request.encoding !== "core-dispatch-ref-v1") {
    const result = readCompletionRequest<ObjectJson>(store, runID, encoded);
    return { ...result, ...(initialWaitSource(result.initialWake) ? { initialWake: readInitialWaitWake(store, runID, result.initialWake, store.requiresRecovery) } : {}),
      ...(result.input ? { input: readRunFrame(store, result.input) } : {}), ...(result.rootAfter ? { rootAfter: readRunInput(store, result.rootAfter) } : {}) } as T;
  }
  const proposal = object(request.proposal);
  if (typeof proposal.operationID !== "string" || proposal.action !== "suspend" || request.decision !== null || request.resolved !== null
      || !store.query("SELECT operation_id FROM history WHERE operation_id=? AND type='core.suspend' AND subject_id=? AND request_json=?",
        [proposal.operationID, runID, encoded as string])[0]) invalid();
  const source = sources(store, runID, request), templates = objects(request.actions);
  if (templates.length !== source.resolved.length) invalid();
  const actions: ObjectJson[] = templates.map((action, index) => {
    if (action.original !== null || action.request !== null) invalid();
    return { ...action, original: copyJson(source.original[index]!), request: copyJson(source.resolved[index]!) };
  });
  const children = objects(request.children).map(child => {
    if (child.input !== null || child.config !== null) invalid();
    const parent = actions.find(action => action.remoteRunID === child.id && action.id === child.parentActionID);
    if (!parent) invalid();
    return { ...child, input: object(parent!.request).input!, config: object(parent!.request).config! };
  });
  const admissionEvidence = objects(request.admissionEvidence).map((entry, index) => {
    if (entry.payload !== null || entry.conclusion !== null) invalid();
    return { ...entry, ...source.admission[index]! };
  });
  if (!same(request.sources, identities(source, actions, children))) invalid();
  const { encoding: _encoding, sources: _sources, ...body } = request;
  return { ...body, ...(initialWaitSource(body.initialWake) ? { initialWake: readInitialWaitWake(store, runID, body.initialWake, store.requiresRecovery) } : {}),
    ...(body.rootAfter ? { rootAfter: readRunInput(store, body.rootAfter) } : {}), decision: source.decision, resolved: source.resolved, actions, children, admissionEvidence } as T;
}
