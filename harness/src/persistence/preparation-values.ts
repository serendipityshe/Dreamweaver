import { canonicalJson, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, validateActionRequest, validatePreparedActionRequest,
  type ActionRequest, type Decision, type Json, type PreparedActionRequest } from "../contract/index.js";
import { readFrameDecision } from "./driver-value.js";
import { readPreparationPolicies, storedPolicySource, verifyPolicySource } from "./policy-values.js";
import { readValidationConclusion, validationValueSource, verifyValidationValueSource } from "./validation-value.js";
import type { SQLiteStore } from "./sqlite.js";

type Row = Record<string, unknown>;
type ObjectJson = { [key: string]: Json };
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid dispatch preparation source"); }
function object(value: unknown): ObjectJson {
  if (!value || typeof value !== "object" || Array.isArray(value)) invalid();
  return value as ObjectJson;
}
function large(value: unknown): boolean {
  let bytes = 0; for (const chunk of canonicalJsonChunks(value)) { bytes += chunk.length; if (bytes > 16_384) return true; }
  return false;
}
function verify(value: unknown, digest: Json | undefined): void { if (typeof digest !== "string" || canonicalJsonDigest(value) !== digest) invalid(); }

export function encodePreparationOriginals(frameID: string, decision: Extract<Decision, { kind: "dispatch" }>, originals: ActionRequest[]): Json {
  if (!large(originals)) return copyJson(originals);
  const source = new Map(decision.actions.map(action => [action.key, action]));
  if (new Set(originals.map(action => action.key)).size !== originals.length) invalid();
  for (const action of originals) { if (!source.has(action.key)) invalid(); verify(action, canonicalJsonDigest(source.get(action.key))); }
  return { encoding: "dispatch-originals-ref-v1", frameID, keys: originals.map(action => action.key), digest: canonicalJsonDigest(originals) };
}

export function readPreparationOriginals(store: SQLiteStore, prep: Row): ActionRequest[] {
  const value = parseJson(prep.original_json as string);
  if (Array.isArray(value)) return value.map(validateActionRequest);
  const source = object(value);
  if (source.encoding !== "dispatch-originals-ref-v1" || source.frameID !== prep.frame_id || !Array.isArray(source.keys)
      || source.keys.some(key => typeof key !== "string") || new Set(source.keys).size !== source.keys.length) invalid();
  const candidate = store.query("SELECT decision_json FROM frame_candidates WHERE frame_id=?", [prep.frame_id as string])[0];
  if (!candidate) invalid();
  const decision = readFrameDecision(store, prep.frame_id as string, candidate.decision_json);
  if (decision.kind !== "dispatch") invalid();
  const actions = new Map(decision.actions.map(action => [action.key, action]));
  const originals = source.keys.map(key => { const action = actions.get(key as string); if (!action) invalid(); return action; });
  verify(originals, source.digest); return originals;
}

export function encodePreparationRewrites(prep: Row, jobs: Row[], rewritten: ActionRequest[]): Json {
  if (!large(rewritten)) return copyJson(rewritten);
  return { encoding: "dispatch-rewrites-ref-v1", frameID: prep.frame_id as string, roundID: prep.round_id as string,
    jobs: jobs.map(job => ({ id: job.id as string, assignmentID: job.assignment_id as string, assignmentEpoch: job.assignment_epoch as number })),
    digest: canonicalJsonDigest(rewritten) };
}

export function readPreparationRewritten(store: SQLiteStore, prep: Row): ActionRequest[] {
  const value = parseJson(prep.rewritten_json as string);
  if (Array.isArray(value)) return value.map(validateActionRequest);
  const source = object(value), originals = readPreparationOriginals(store, prep);
  if (source.encoding === "dispatch-originals-ref-v1") {
    if (canonicalJsonDigest(source) !== canonicalJsonDigest(parseJson(prep.original_json as string))) invalid();
    return originals;
  }
  if (source.encoding !== "dispatch-rewrites-ref-v1" || source.frameID !== prep.frame_id || source.roundID !== prep.round_id || !Array.isArray(source.jobs)) invalid();
  const round = store.query("SELECT principal_id FROM prepare_rounds WHERE id=? AND owner_kind='frame' AND owner_id=? AND phase='dispatch'", [prep.round_id as string, prep.frame_id as string])[0];
  if (!round) invalid();
  const byKey = new Map(originals.map(action => [action.key, action])), rewritten = new Map<string, ActionRequest>(), seen = new Set<string>();
  for (const item of source.jobs) {
    const ref = object(item);
    if (typeof ref.id !== "string" || seen.has(ref.id) || typeof ref.assignmentID !== "string" || !Number.isSafeInteger(ref.assignmentEpoch)) invalid();
    seen.add(ref.id);
    const row = store.query("SELECT payload_json,report_json FROM validation_jobs WHERE id=? AND round_id=? AND state='accepted' AND assignment_id=? AND assignment_epoch=?",
      [ref.id, prep.round_id as string, ref.assignmentID, ref.assignmentEpoch as number])[0];
    const receipt = store.query("SELECT request_json,result_json FROM history WHERE operation_id=? AND type='validation.reported' AND subject_id=?",
      [`node.report:${canonicalJson([ref.assignmentID])}`, ref.id])[0];
    if (!row || !receipt) invalid();
    const storedPayload = parseJson(row.payload_json as string), payload = object(storedPayload), report = object(readValidationConclusion(store, row.report_json));
    const context = object(payload.context);
    const policiesRecord = parseJson(prep.policies_json as string), policySource = storedPolicySource(policiesRecord);
    if (policySource) {
      verifyPolicySource(store, policySource, context.policies);
      if (canonicalJsonDigest(context.policySource) !== canonicalJsonDigest(policySource)) invalid();
    }
    if (context.domainID !== store.domainID || context.frameID !== prep.frame_id || payload.principalID !== round.principal_id
        || canonicalJsonDigest(context.policies) !== canonicalJsonDigest(policySource ? object(policiesRecord).policies : policiesRecord)) invalid();
    const request = object(parseJson(receipt.request_json as string)), assignment = object(request.assignment), result = object(parseJson(receipt.result_json as string));
    const conclusionSource = validationValueSource(row.report_json);
    if (conclusionSource) verifyValidationValueSource(store, conclusionSource);
    const payloadMatches = request.encoding === "validation-report-ref-v1"
      ? assignment.payload === null && request.assignmentPayloadDigest === canonicalJsonDigest(storedPayload)
      : request.encoding === undefined && canonicalJsonDigest(assignment.payload) === canonicalJsonDigest(storedPayload);
    if (assignment.jobID !== ref.id || assignment.roundID !== prep.round_id || assignment.assignmentID !== ref.assignmentID || assignment.assignmentEpoch !== ref.assignmentEpoch
        || !payloadMatches || (conclusionSource ? request.conclusion !== null || canonicalJson(request.conclusionSource) !== canonicalJson(conclusionSource)
          || canonicalJson(result.conclusion) !== canonicalJson(conclusionSource) : canonicalJsonDigest(request.conclusion) !== canonicalJsonDigest(report)
          || canonicalJsonDigest(result.conclusion) !== canonicalJsonDigest(report)) || payload.phase !== "rewrite" || report.valid !== true
        || !Array.isArray(payload.value) || !Array.isArray(report.rewritten) || payload.value.length !== report.rewritten.length) invalid();
    for (let i = 0; i < payload.value.length; i++) {
      const original = validateActionRequest(payload.value[i]), revised = validateActionRequest(report.rewritten[i]);
      if (!byKey.has(original.key) || rewritten.has(original.key) || original.key !== revised.key || original.kind !== revised.kind) invalid();
      verify(original, canonicalJsonDigest(byKey.get(original.key)));
      rewritten.set(original.key, revised);
    }
  }
  if (rewritten.size !== originals.length) invalid();
  const ordered = originals.map(action => rewritten.get(action.key)!);
  verify(ordered, source.digest); return ordered;
}

export function encodePreparationResolved(prep: Row, resolved: PreparedActionRequest[]): Json {
  if (!large(resolved)) return copyJson(resolved);
  return { encoding: "dispatch-resolved-ref-v1", frameID: prep.frame_id as string, roundID: prep.round_id as string,
    generation: (prep.generation as number) + 1, versions: resolved.map(action => action.target.version), digest: canonicalJsonDigest(resolved) };
}

export function readPreparationResolved(store: SQLiteStore, prep: Row): PreparedActionRequest[] {
  const value = parseJson(prep.resolved_json as string);
  if (Array.isArray(value)) return value.map(validatePreparedActionRequest);
  const source = object(value);
  if (source.encoding !== "dispatch-resolved-ref-v1" || source.frameID !== prep.frame_id || source.roundID !== prep.round_id
      || source.generation !== prep.generation || !Array.isArray(source.versions) || source.versions.some(version => typeof version !== "string")) invalid();
  const rewritten = readPreparationRewritten(store, prep), policies = readPreparationPolicies(store, prep);
  if (rewritten.length !== source.versions.length || !Array.isArray(policies)) invalid();
  const versions = source.versions;
  const resolved = rewritten.map((action, i) => validatePreparedActionRequest({ ...action, target: { id: action.target.id, version: versions[i] }, policies }));
  verify(resolved, source.digest); return resolved;
}
