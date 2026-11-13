import { encodeRunInput, expectedRunSnapshot } from "../persistence/index.js";
import { readRunSnapshot } from "../persistence/index.js";
import { stageCommandValue, abandonCommandValue, commandPublicationWrites, retireCommandValues, encodeCommandRequest, commandRequestMetadata, readCommandRequest, readCommandSource, commandSource, verifyCommandValue, commandProjection, readCommandProjectionSync, type CommandValueSource, type CommandBinding } from "../persistence/index.js";
import { verifyCanonicalValue } from "../persistence/index.js";
import { encodeCheckpointFields, expandCheckpointFields } from "../persistence/index.js";
import { compactPolicyPayload } from "../contract/index.js";
import { policyPayloadSource, storedPolicySource, compactActionRequestPayload } from "../persistence/index.js";
import { encodeActionSnapshot, encodeActionRequests, expandActionRequests } from "../persistence/index.js";
import { readActionCommandReceipt } from "../persistence/index.js";
import { readValidationConclusionAsync } from "../persistence/index.js";
﻿import { createHash, randomUUID } from "node:crypto";
import { assertJsonValueSize, canonicalJson, canonicalJsonDigest, canonicalJsonChunks, compactReportPayload, copyJson, harnessError, HarnessError, jsonEqual, parseJson, validateReport, type ActionObservation, type ActionView, type ErrorInfo, type Fact, type Json, type ResolveActionRequest, type RunView } from "../contract/index.js";
import { authorizeOperation, type PrincipalGrant } from "../message/index.js";
import { actionReportValueReference, readActionReport, readActionSnapshot, readActionControl, readFactOutcome, cancellationGuard, type ActionControl, type CommitBatch, type SQLiteStore, type SQLWrite } from "../persistence/index.js";
import { reconciledEffectGuard } from "./action-effect.js";

type Row = ReturnType<SQLiteStore["query"]>[number];
const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
const commandID = (principalID: string, requestKey: string) => `resolve:${canonicalJson([principalID, requestKey])}`;
function sameCommand(left: unknown, right: unknown): boolean {
  const a = canonicalJsonChunks(left)[Symbol.iterator](), b = canonicalJsonChunks(right)[Symbol.iterator]();
  for (;;) { const x = a.next(), y = b.next(); if (x.done || y.done) return x.done === y.done; if (!Buffer.from(x.value).equals(y.value)) return false; }
}

/** Durable operator report processing. No plugin code executes in this service. */
export function createResolveService(store: SQLiteStore, options: {
  clock: { now(): number }; prepareTimeoutMs: number; retryAfterMs: number; pollMs: number; maxValueBytes?: number; maxResponseBytes?: number; valueQuota?: number;
  authorizeAction(id: string, principal: PrincipalGrant): ActionView;
  authorizeActionControl?(id: string, principal: PrincipalGrant): ActionControl;
  authorizeOperator(principalID: string, ownerID: string, action: ActionView): void;
  authorizeModelRetry?(action: ActionView): void;
}) {
  let closed = false, scanning = false, timer: ReturnType<typeof setInterval> | undefined;
  let cursor: [number, string, string] | undefined;
  const now = () => { const at = options.clock.now(); if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock"); return at; };
  const authorize = (id: string, principal: PrincipalGrant) => options.authorizeActionControl
    ? options.authorizeActionControl(id, principal) : options.authorizeAction(id, principal);
  function command(principal: string, key: string) { return store.query("SELECT * FROM commands WHERE principal_id=? AND request_key=?", [principal, key])[0]; }
  async function current(actionID: string) {
    const row = store.query("SELECT * FROM actions WHERE id=?", [actionID])[0];
    if (!row?.snapshot_json) throw harnessError("NOT_FOUND", "Action not found");
    if (row.snapshot_projection_id !== null) throw harnessError("STATE_CONFLICT", "Referenced Action management is not yet available");
    const action = await readActionSnapshot(store, row);
    const runRow = store.query("SELECT * FROM runs WHERE id=?", [action.runID])[0];
    const work = store.query("SELECT * FROM work_items WHERE kind='action' AND subject_id=?", [action.id])[0];
    if (!runRow?.snapshot_json || !work || row.revision !== action.revision) throw harnessError("INTERNAL_ERROR", "Action state is inconsistent");
    return { row, action, runRow, run: readRunSnapshot(store, runRow.snapshot_json), work };
  }
  async function stateError(action: ActionView, run: RunView, work: Row, request: ResolveActionRequest, pending = false): Promise<HarnessError | null> {
    if (action.revision !== request.expectedRevision) return harnessError("REVISION_CONFLICT", "Action revision changed");
    if (action.request.kind === "agent") return harnessError("INVALID_ARGUMENT", "Agent Action must be managed through its child Run");
    if (request.resolution.kind === "legacy_report") {
      if (action.recheckRequested || !(pending ? work.stage === "operator_pending" : ["blocked", "done", "report_pending"].includes(work.stage as string))) return harnessError("STATE_CONFLICT", "Action has an active or queued round");
      return null;
    }
    if (action.recheckRequested || !(pending ? work.stage === "operator_pending" : ["blocked", "done"].includes(work.stage as string))) return harnessError("STATE_CONFLICT", "Action has an active or queued round");
    if (action.outcome && (action.outcome.kind !== "cancelled" || !action.blocked || action.executionCount === 0)) return harnessError("ACTION_TERMINAL", "Action has no unresolved effect");
    if (!action.outcome && !action.blocked) return harnessError("STATE_CONFLICT", "Action is not blocked");
    if ((request.resolution.kind === "allow_retry" || request.resolution.kind === "allow_model_retry")) {
      if (request.resolution.kind === "allow_model_retry") {
        if (!options.authorizeModelRetry) return harnessError("FORBIDDEN", "Model retry is not enabled");
        options.authorizeModelRetry(action);
        if (action.executionCount === 0) return harnessError("RECONCILIATION_REQUIRED", "Model Action has not executed");
      }
      if (action.outcome || run.outcome || store.effectiveCancellation({ kind: "action", id: action.id })) return harnessError("ACTION_TERMINAL", "Cancelled or terminal Action cannot execute");
      if (action.executionCount >= run.limits.maxExecutionsPerAction) return harnessError("LIMIT_EXCEEDED", "Action execution budget exhausted");
      if (!await safeRetryReport(action, request.resolution.kind === "allow_model_retry")) return harnessError("RECONCILIATION_REQUIRED", "Safe retry evidence is unavailable");
    }
    return null;
  }
  async function legacyReport(request: ResolveActionRequest) {
    const resolution = request.resolution;
    if (resolution.kind !== "legacy_report") throw harnessError("INVALID_ARGUMENT", "Not a legacy report request");
    const row = store.query(`SELECT r.*,p.state processing_state,p.revision processing_revision,s.report_value_limit
      FROM action_reports r JOIN report_processing p ON p.report_id=r.id
      JOIN action_attempt_state s ON s.attempt_id=r.attempt_id AND s.action_id=r.action_id
      WHERE r.id=? AND r.action_id=? AND r.attempt_id=? AND r.phase=?`, [resolution.reportID, request.actionID, resolution.attemptID, resolution.phase])[0];
    const unknown = store.query("SELECT id FROM history WHERE subject_id=? AND type='action.report_limit_unknown' AND json_extract(data_json,'$.reportID')=? AND json_extract(data_json,'$.attemptID')=?", [request.actionID, resolution.reportID, resolution.attemptID])[0];
    if (!row || row.assignment_id !== resolution.attemptID || row.processing_state !== "deferred" || row.report_value_limit !== null || !unknown) throw harnessError("STATE_CONFLICT", "Deferred legacy report is unavailable");
    const ref = actionReportValueReference(store, row);
    const envelope = await readActionReport(store, row);
    const digest = ref?.digest ?? createHash("sha256").update(canonicalJson(envelope)).digest("hex");
    if (envelope.kind !== "report" || envelope.value === undefined || !/^[0-9a-f]{64}$/.test(resolution.reportDigest) || digest !== resolution.reportDigest) throw harnessError("INVALID_ARGUMENT", "Legacy report digest does not match");
    const checkSize = (value: Json) => { let size = 0; for (const chunk of canonicalJsonChunks(value)) { size += chunk.byteLength; if (size > resolution.maxValueBytes) throw harnessError("LIMIT_EXCEEDED", "JSON value exceeds compatibility maxValueBytes"); } };
    checkSize(envelope.value);
    const report = resolution.phase === "execute" ? validateReport(envelope.value, "execute") : validateReport(envelope.value, "reconcile");
    const result = resolution.phase === "execute" ? report.payload : report.payload.kind === "settled" ? report.payload.result : null;
    if (!result || !(result.kind === "succeeded" || result.kind === "failed")) throw harnessError("RECONCILIATION_REQUIRED", "Legacy report has no determinate result");
    checkSize(report.evidence);
    if (result.kind === "succeeded") checkSize(result.value);
    return { row, ref, report, result, reported: envelope.value };
  }
  async function safeRetryReport(action: ActionView, modelRisk = false): Promise<Row | null> {
    const observation = action.latestObservation;
    if (!observation || observation.source !== "reconcile" || !observation.validated || observation.validationError || !observation.attemptID
        || action.latestAttempt?.id !== observation.attemptID || action.latestAttempt.mode !== "recheck-only" || action.latestAttempt.finishedAt === null) return null;
    let report: ReturnType<typeof validateReport>;
    try { report = validateReport(observation.reported, "reconcile"); } catch { return null; }
    if (report.payload.kind !== (modelRisk ? "unresolved" : "retryable")) return null;
    const row = store.query("SELECT r.*,p.state processing_state,s.mode FROM action_reports r JOIN report_processing p ON p.report_id=r.id JOIN action_attempt_state s ON s.attempt_id=r.attempt_id WHERE r.id=? AND r.action_id=? AND r.attempt_id=?", [observation.id, action.id, observation.attemptID])[0];
    if (!row || row.phase !== "reconcile" || row.authority_json === null || row.processing_state !== "done" || row.mode !== "recheck-only") return null;
    try {
      const recorded = await readActionReport(store, row);
      if (recorded.kind !== "report") return null;
      const hash = (value: Json) => { const digest = createHash("sha256"); for (const part of canonicalJsonChunks(value)) digest.update(part); return digest.digest("hex"); };
      return hash(recorded.value) === hash(observation.reported) ? row : null;
    } catch { return null; }
  }
  function receipt(row: Row): ActionView | null {
    if (row.status === "rejected") throw new HarnessError(decode<ErrorInfo>(row.error_json));
    return row.status === "applied" ? readInlineReceipt(row.result_json) : null;
  }
  function readInlineReceipt(encoded: unknown): ActionView {
    const value = readCommandProjectionSync(store, encoded), actionID = (value as Record<string, Json>).id;
    if (typeof actionID !== "string") throw harnessError("INTERNAL_ERROR", "Operator receipt has no target");
    const body = typeof encoded === "string" ? parseJson(encoded) : encoded;
    if (body && typeof body === "object" && !Array.isArray(body) && (body as Record<string, unknown>).encoding === "command-projection-v1") return expandCheckpointFields(store, expandActionRequests(store, value)) as unknown as ActionView;
    return readActionCommandReceipt(store, value, actionID);
  }
  function receiptStorage(value: ActionView): Json { return encodeActionSnapshot(value); }
  function reject(principal: string, request: ResolveActionRequest, row: Row, round: Row | null, failure: HarnessError, actionRow: Row, work: Row) {
    const at = now();
    const writes: SQLWrite[] = [write("UPDATE commands SET status=?,error_json=?,updated_at=? WHERE principal_id=? AND request_key=? AND operation='resolveAction' AND status='pending' AND request_json=?", ["rejected", { json: json(failure.info) }, at, principal, request.requestKey, row.request_json as string])];
    const control = store.query("SELECT * FROM work_items WHERE id=? AND kind='operator'", [`${commandID(principal, request.requestKey)}:work`])[0];
    if (!control) throw harnessError("INTERNAL_ERROR", "Operator control work is unavailable");
    const previous = decode<{ stage: string }>(control.payload_json).stage;
    writes.push(write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [request.actionID, actionRow.revision as number, actionRow.snapshot_json as string]),
      write("UPDATE work_items SET stage=? WHERE id=? AND kind='operator' AND stage='pending'", ["done", control.id as string]));
    if (work.stage === "operator_pending") writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND stage='operator_pending'", [previous, work.id as string]));
    if (round) writes.push(write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at=?", ["expired", round.id as string, "pending", round.deadline_at as number]));
    store.commit({ operationID: `${commandID(principal, request.requestKey)}:reject`, kind: "action.resolve_rejected", subjectID: request.actionID, request: parseJson(row.request_json as string), writes, evidence: json(failure.info), result: null });
  }
  async function preparation(request: ResolveActionRequest, principal: string, action: ActionView, run: RunView, owner: string, frameID: string, source = commandSource(command(principal, request.requestKey)?.request_json), existing?: unknown) {
    if ((request.resolution.kind === "allow_retry" || request.resolution.kind === "allow_model_retry")) throw harnessError("RECONCILIATION_REQUIRED", "Safe retry authorization is unavailable");
    const legacy = request.resolution.kind === "legacy_report" ? await legacyReport(request) : null;
    const result = legacy?.result ?? (request.resolution.kind === "settle" ? request.resolution.result : null);
    if (!result) throw harnessError("RECONCILIATION_REQUIRED", "Report result is unavailable");
    const report = legacy?.reported ?? (request.resolution.kind === "settle" ? { payload: request.resolution.result, evidence: request.evidence, measurements: request.resolution.measurements } : null);
    const reportID = commandID(principal, request.requestKey);
    const payload = { kind: "capacity", ref: action.request.target, phase: "report-output", value: source || legacy?.ref ? null : result.kind === "succeeded" ? result.value : null,
      config: action.request.config, principalID: principal, operation: "admin.resolveAction", context: { domainID: store.domainID, rootID: run.rootID, runID: run.id, frameID,
        actionID: action.id, reportID, reportPhase: "operator", ownerPrincipalID: owner, command: source ? null : request,
        ...(source ? { operatorSource: source } : {}),
        ...(legacy?.ref ? { reportEncoding: "report-ref-v1", originalReportRef: legacy.ref } : source && request.resolution.kind === "settle" ? {} : { originalReport: report }), prepared: action.request, policies: action.request.policies } };
    const policySource = existing === undefined ? policyPayloadSource(store, payload) : storedPolicySource((parseJson(existing as string) as Record<string, Json>).context);
    return compactReportPayload(compactPolicyPayload(compactActionRequestPayload(store, payload, existing), policySource as unknown as Json ?? undefined), existing);
  }
  function checkRequestValues(request: ResolveActionRequest) {
    if (options.maxValueBytes !== undefined) {
      assertJsonValueSize(request.evidence, options.maxValueBytes);
      if (request.resolution.kind === "settle") {
        assertJsonValueSize(json({ payload: request.resolution.result, evidence: request.evidence, measurements: request.resolution.measurements }), options.maxValueBytes);
        if (request.resolution.result.kind === "succeeded") assertJsonValueSize(request.resolution.result.value, options.maxValueBytes);
      }
    }
  }
  function checkSettlementBudget(request: ResolveActionRequest, principal: string, owner: string, action: ActionView) {
    if (request.resolution.kind !== "settle") return;
    if (options.maxResponseBytes === undefined) return;
    const max = Number.MAX_SAFE_INTEGER, observationID = "00000000-0000-0000-0000-000000000000";
    const result = request.resolution.result;
    const observation: ActionObservation = { id: observationID, attemptID: null, source: "operator", observedAt: max, result, evidence: request.evidence,
      validationError: null, measurements: request.resolution.measurements, reported: json({ payload: result, evidence: request.evidence, measurements: request.resolution.measurements }), validated: true, acceptedAsOutcome: true };
    const next: ActionView = { ...action, revision: max, updatedAt: max, latestObservation: observation, status: "settled", outcome: action.outcome ?? result, outcomeObservationID: observationID, settledAt: max, blocked: null };
    const cursor = Buffer.from(canonicalJson({ high: max, position: max })).toString("base64url") + "." + "A".repeat(43);
    const history = { id: observationID, sequence: max, at: max, runID: action.runID, actionID: action.id, attemptID: null, type: "action.observed", data: { actionID: action.id, observation } };
    const command = { requestKey: request.requestKey, operation: "resolveAction", status: "applied", result: next, error: null, createdAt: max, updatedAt: max };
    const envelope = (subject: string, value: unknown) => ({ protocolVersion: "harness/1", requestID: observationID, binding: { domainID: store.domainID, principalID: subject }, ok: true, result: value });
    if ([envelope(principal, next), envelope(principal, command), envelope(owner, next), envelope(owner, { items: [next], nextCursor: cursor }), envelope(owner, { items: [history], nextCursor: cursor })]
      .some(value => Buffer.byteLength(canonicalJson(value), "utf8") > options.maxResponseBytes!)) {
      throw harnessError("LIMIT_EXCEEDED", "Operator settlement cannot fit in the response budget");
    }
  }
  async function register(request: ResolveActionRequest, principal: string, fragmentedResponses: boolean, revalidate: () => PrincipalGrant) {
    const epoch = store.coordinatorEpoch;
    const authorizeCurrent = () => {
      store.assertAuthority();
      if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Operator authority changed");
      const grant = revalidate();
      if (grant.principalID !== principal) throw harnessError("FORBIDDEN", "Operator identity changed");
      authorizeOperation(grant, "admin.resolveAction"); authorize(request.actionID, grant);
    };
    const legacySource = request.resolution.kind === "legacy_report" ? await legacyReport(request) : null;
    let largeLegacy = legacySource?.ref !== null && legacySource?.ref !== undefined;
    if (legacySource && !largeLegacy) { let bytes = 0; for (const part of canonicalJsonChunks(legacySource.reported)) { bytes += part.length; if (bytes > 16_384) { largeLegacy = true; break; } } }
    const source = await stageCommandValue(store, principal, request, options.valueQuota ?? Number.MAX_SAFE_INTEGER, authorizeCurrent, largeLegacy);
    const encoded = encodeCommandRequest(request, source);
    const publish = (batch: CommitBatch) => store.commit({ ...batch, request: encoded,
      writes: [...batch.writes ?? [], ...commandPublicationWrites(source, epoch)], ...(source ? { valuePublications: [source.value] } : {}) });
    try {
    // Agent Actions have no Capacity work item. Reject with the same durable
    // command identity as other deterministic management refusals.
    const target = store.query("SELECT * FROM actions WHERE id=?", [request.actionID])[0];
    if (target && target.snapshot_projection_id !== null) {
      const control = readActionControl(store, request.actionID);
      const work = store.query("SELECT stage FROM work_items WHERE kind='action' AND subject_id=?", [control.id])[0];
      const failure = control.revision !== request.expectedRevision ? harnessError("REVISION_CONFLICT", "Action revision changed")
        : control.kind === "agent" ? harnessError("INVALID_ARGUMENT", "Agent Action must be managed through its child Run")
        : request.resolution.kind !== "legacy_report" && work?.stage === "done" && !control.recheckRequested && control.outcomeKind !== null
          && (control.outcomeKind !== "cancelled" || control.blockedCode === null || control.executionCount === 0)
          ? harnessError("ACTION_TERMINAL", "Action has no unresolved effect") : null;
      if (!failure) throw harnessError("STATE_CONFLICT", "Referenced Action management is not yet available");
      const at = now();
      authorizeCurrent();
      publish({ operationID: `${commandID(principal, request.requestKey)}:register`, kind: "action.resolve_registered", subjectID: control.id,
        request: json(request), evidence: json(failure.info), result: null, writes: [
          write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_projection_id=?", [control.id, control.revision, target.snapshot_projection_id as string]),
          ...(work ? [write("UPDATE work_items SET stage=? WHERE kind='action' AND subject_id=? AND stage=?", [work.stage as string, control.id, work.stage as string])] : []),
          write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            [principal, request.requestKey, "resolveAction", { json: encoded }, canonicalJsonDigest(request), "rejected", null, { json: json(failure.info) }, at, at]),
        ] });
      throw failure;
    }
    const targetAction = target?.snapshot_json ? await readActionSnapshot(store, target) : null;
    if (targetAction?.request.kind === "agent") {
      if (targetAction.revision !== target!.revision) throw harnessError("REVISION_CONFLICT", "Action revision changed");
      const failure = targetAction.revision !== request.expectedRevision ? harnessError("REVISION_CONFLICT", "Action revision changed")
        : harnessError("INVALID_ARGUMENT", "Agent Action must be managed through its child Run");
      const at = now();
      authorizeCurrent();
      publish({ operationID: `${commandID(principal, request.requestKey)}:register`, kind: "action.resolve_registered", subjectID: targetAction.id,
        request: json(request), evidence: json(failure.info), result: null, writes: [
          write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [targetAction.id, targetAction.revision, target!.snapshot_json as string]),
          write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            [principal, request.requestKey, "resolveAction", { json: encoded }, canonicalJsonDigest(request), "rejected", null, { json: json(failure.info) }, at, at]),
        ] });
      throw failure;
    }
    if (request.resolution.kind === "legacy_report") await legacyReport(request);
    const { row, action, runRow, run, work } = await current(request.actionID);
    const at = now();
    const lease = store.query("SELECT * FROM business_leases WHERE kind='action' AND subject_id=?", [action.id])[0];
    const active = !!lease && lease.released === 0 && (lease.expires_at as number) > at;
    const failure = await stateError(action, run, work, request) ?? (active ? harnessError("STATE_CONFLICT", "Action has an active processing round") : null);
    if (!failure && !fragmentedResponses) checkSettlementBudget(request, principal, runRow.principal_id as string, action);
    const deadline = at + options.prepareTimeoutMs;
    if (!Number.isSafeInteger(deadline)) throw harnessError("LIMIT_EXCEEDED", "Preparation deadline exceeds supported range");
    const fingerprint = source?.value.digest ?? canonicalJsonDigest(request), id = commandID(principal, request.requestKey);
    const writes: SQLWrite[] = [write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [action.id, action.revision, row.snapshot_json as string]),
      write("UPDATE work_items SET stage=? WHERE id=? AND kind='action' AND subject_id=? AND stage=?", [work.stage as string, work.id as string, action.id, work.stage as string]),
      write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", [principal, request.requestKey, "resolveAction", { json: encoded }, fingerprint, failure ? "rejected" : "pending", null, failure ? { json: json(failure.info) } : null, at, at])];
    // A fresh lease always increments the Action epoch; existing leases also
    // need their expiration and release state checked under this write lock.
    writes.unshift(write("UPDATE actions SET epoch=epoch+0 WHERE id=? AND epoch=?", [action.id, row.epoch as number]));
    if (lease) writes.unshift(write("UPDATE business_leases SET expires_at=expires_at+0 WHERE kind='action' AND subject_id=? AND epoch=? AND released=? AND expires_at=?", [action.id, lease.epoch as number, lease.released as number, lease.expires_at as number]));
    if (!failure && (request.resolution.kind === "allow_retry" || request.resolution.kind === "allow_model_retry")) {
      const proof = await safeRetryReport(action, request.resolution.kind === "allow_model_retry");
      if (!proof) throw harnessError("RECONCILIATION_REQUIRED", "Safe retry evidence disappeared");
      const retryID = `operator.retry:${action.id}`;
      const old = store.query("SELECT * FROM work_items WHERE id=? AND kind='operator_retry'", [retryID])[0];
      const next: ActionView = { ...action, revision: action.revision + 1, updatedAt: at, blocked: null };
      const commandWrite = writes.findIndex(item => item.sql.startsWith("INSERT INTO commands"));
      writes[commandWrite] = write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", [principal, request.requestKey, "resolveAction", { json: encoded }, fingerprint, "applied", { json: receiptStorage(next) }, null, at, at]);
      writes.unshift(cancellationGuard({ kind: "action", id: action.id }),
        write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND business_cancelled=0 AND business_terminal=0", [run.id, run.revision, runRow.snapshot_json as string]),
        write("UPDATE report_processing SET state=? WHERE report_id=? AND state=?", ["done", proof.id as string, "done"]));
      writes.push(write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS NULL", [next.revision, { json: encodeActionSnapshot(next) }, action.id, action.revision, row.snapshot_json as string]),
        write("UPDATE work_items SET stage=?,available_at=? WHERE id=? AND kind='action' AND subject_id=? AND stage=?", ["accepted", at, work.id as string, action.id, work.stage as string]));
      const payload = json({ principalID: principal, requestKey: request.requestKey, reportID: proof.id });
      writes.push(old ? write("UPDATE work_items SET stage=?,payload_json=?,result_json=NULL WHERE id=? AND kind='operator_retry' AND stage=? AND payload_json=?", ["pending", { json: payload }, retryID, old.stage as string, old.payload_json as string])
        : write("INSERT INTO work_items(id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES(?,?,?,?,?,?,?,?)", [retryID, "operator_retry", action.id, "pending", 0, at, { json: payload }, null]));
      options.authorizeOperator(principal, runRow.principal_id as string, action);
      if (request.resolution.kind === "allow_model_retry") options.authorizeModelRetry!(action);
      authorizeCurrent();
      publish({ operationID: `${id}:register`, kind: "action.retry_authorized", subjectID: action.id, request: encoded, writes, result: receiptStorage(next), evidence: commandProjection(source, { reportID: proof.id, operatorEvidence: request.evidence }, [{ path: ["operatorEvidence"], selector: "evidence" }]),
        events: [{ type: "resolution.applied", subjectID: action.id, data: json({ principalID: principal, requestKey: request.requestKey, reportID: proof.id, resolution: request.resolution.kind }) }] });
      return;
    }
    if (!failure) {
      const accepted = store.query("SELECT data_json FROM history WHERE subject_id=? AND type='action.accepted' ORDER BY sequence LIMIT 1", [action.id])[0];
      if (!accepted) throw harnessError("INTERNAL_ERROR", "Action admission evidence is unavailable");
      const frameID = decode<{ frameID: string }>(accepted.data_json).frameID, roundID = randomUUID();
      const payload = await preparation(request, principal, action, run, runRow.principal_id as string, frameID, source);
      writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND stage=?", ["operator_pending", work.id as string, work.stage as string]),
        write("INSERT INTO work_items(id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES(?,?,?,?,?,?,?,?)", [`${id}:work`, "operator", action.id, "pending", 0, at, { json: json({ stage: work.stage }) }, null]),
        write("INSERT INTO prepare_rounds(id,owner_kind,owner_id,principal_id,phase,number,prepare_timeout_ms,created_at,deadline_at,state) VALUES(?,?,?,?,?,?,?,?,?,?)", [roundID, "command", id, principal, "operator-output", 1, options.prepareTimeoutMs, at, deadline, "pending"]),
        write("INSERT INTO validation_jobs(id,round_id,payload_json,state) VALUES(?,?,?,?)", [randomUUID(), roundID, { json: json(payload) }, "pending"]));
    }
    if (!failure) options.authorizeOperator(principal, runRow.principal_id as string, action);
    authorizeCurrent();
    publish({ operationID: `${id}:register`, kind: "action.resolve_registered", subjectID: action.id, request: encoded, writes, result: null, evidence: failure ? json(failure.info) : json({ deadline }), ...(active && failure?.info.code === "STATE_CONFLICT" ? { validBefore: lease!.expires_at as number } : {}) });
    if (failure) throw failure;
    } finally { abandonCommandValue(store, source); }
  }
  async function process(row: Row) {
    const principal = row.principal_id as string, metadata = commandRequestMetadata(row.request_json), id = commandID(principal, metadata.requestKey), source = commandSource(row.request_json);
    if (row.operation !== "resolveAction" || row.status !== "pending" || (metadata.resolution.kind === "allow_retry" || metadata.resolution.kind === "allow_model_retry")) return;
    const round = store.query("SELECT * FROM prepare_rounds WHERE owner_kind='command' AND owner_id=? AND state='pending'", [id])[0];
    if (!round) return;
    const { row: actionRow, action, runRow, run, work } = await current(metadata.actionID);
    // An already changed Action is a deterministic conflict even if this scan
    // resumes after the validation deadline or the operator loses permission.
    const error = await stateError(action, run, work, metadata, true);
    if (error) { reject(principal, metadata, row, round, error, actionRow, work); return; }
    const at = now();
    if (at >= (round.deadline_at as number)) { reject(principal, metadata, row, round, harnessError("VERSION_UNAVAILABLE", "Operator validation expired", { roundID: round.id as string, deadlineAt: round.deadline_at as number }, "rejected", options.retryAfterMs), actionRow, work); return; }
    options.authorizeOperator(principal, runRow.principal_id as string, action);
    const job = store.query("SELECT * FROM validation_jobs WHERE round_id=? AND state='accepted'", [round.id as string])[0];
    const accepted = store.query("SELECT data_json FROM history WHERE subject_id=? AND type='action.accepted' ORDER BY sequence LIMIT 1", [action.id])[0];
    if (!job || !accepted) return;
    const request = source ? await readCommandSource(store, source) : metadata;
    options.authorizeOperator(principal, runRow.principal_id as string, action);
    const frameID = decode<{ frameID: string }>(accepted.data_json).frameID;
    if (job.payload_json !== canonicalJson(await preparation(request, principal, action, run, runRow.principal_id as string, frameID, undefined, job.payload_json))) return;
    const validation = await readValidationConclusionAsync(store, job.report_json);
    options.authorizeOperator(principal, runRow.principal_id as string, action);
    if (!validation.valid && validation.issue?.code !== "OUTPUT_VALIDATION_FAILED") return;
    const legacy = request.resolution.kind === "legacy_report" ? await legacyReport(request) : null;
    const result = legacy?.result ?? (request.resolution.kind === "settle" ? request.resolution.result : null);
    if (!result) return;
    const validationError = validation.valid ? null : { code: "OUTPUT_VALIDATION_FAILED", message: "Capacity output validation failed", details: null };
    const outcome = validationError ? { kind: "failed" as const, error: validationError } : result;
    const cancellation = store.effectiveCancellation({ kind: "action", id: action.id });
    const adopt = !action.outcome && !run.outcome && !cancellation && (!legacy || action.latestAttempt?.id === legacy.row.attempt_id);
    const control = store.query("SELECT * FROM work_items WHERE id=? AND kind='operator' AND stage='pending'", [`${id}:work`])[0];
    if (!control) return;
    const previousStage = decode<{ stage: string }>(control.payload_json).stage;
    const observationID = randomUUID();
    const observation: ActionObservation = { id: observationID, attemptID: legacy ? legacy.row.attempt_id as string : null, source: legacy ? legacy.row.phase as "execute" | "reconcile" : "operator", observedAt: at, result, evidence: legacy?.report.evidence ?? request.evidence,
      validationError, measurements: legacy?.report.measurements ?? (request.resolution.kind === "settle" ? request.resolution.measurements : []), reported: legacy?.reported ?? json({ payload: result, evidence: request.evidence, measurements: request.resolution.kind === "settle" ? request.resolution.measurements : [] }), validated: validation.valid, acceptedAsOutcome: adopt && validation.valid };
    const next: ActionView = { ...action, revision: action.revision + 1, updatedAt: at, latestObservation: observation };
    if (adopt) Object.assign(next, { status: "settled", outcome, outcomeObservationID: observationID, settledAt: at, blocked: null });
    const knownEffect = !adopt ? await reconciledEffectGuard(store, action, observation) : null;
    if (knownEffect) next.blocked = null;
    const receiptView: ActionView = cancellation && !next.outcome ? { ...next, cancelRequested: true, cancellation } : next;
    const observationBindings: CommandBinding[] = source && request.resolution.kind !== "allow_retry" && request.resolution.kind !== "allow_model_retry" ? [
      { path: ["result"], selector: "result" }, { path: ["evidence"], selector: "evidence" },
      { path: ["measurements"], selector: "measurements" }, { path: ["reported"], selector: "report" },
    ] : [];
    const storedObservation = observationBindings.length ? commandProjection(source, observation, observationBindings) : json(observation);
    const receiptBindings: CommandBinding[] = observationBindings.map(binding => ({ ...binding, path: ["latestObservation", ...binding.path] }));
    if (source && request.resolution.kind !== "allow_retry" && request.resolution.kind !== "allow_model_retry" && adopt && validation.valid) receiptBindings.push({ path: ["outcome"], selector: "result" });
    const receiptProjection = receiptBindings.length ? commandProjection(source, receiptView, receiptBindings) as Record<string, Json> : null;
    const storedReceipt = receiptProjection ? { ...receiptProjection, template: receiptStorage(receiptProjection.template as unknown as ActionView) } : receiptStorage(receiptView);
    const storedOutcome = source && request.resolution.kind !== "allow_retry" && request.resolution.kind !== "allow_model_retry" && adopt && validation.valid
      ? { ...commandProjection(source, null, [{ path: [], selector: "result" }]) as Record<string, Json>, kind: outcome.kind } : json(outcome);
    const writes: SQLWrite[] = [write("UPDATE commands SET status=?,result_json=?,updated_at=? WHERE principal_id=? AND request_key=? AND operation='resolveAction' AND status='pending' AND request_json=?", ["applied", { json: storedReceipt }, at, principal, request.requestKey, row.request_json as string]),
      write("UPDATE prepare_rounds SET state=? WHERE id=? AND state='pending' AND deadline_at>?", ["accepted", round.id as string, at]),
      write("UPDATE validation_jobs SET state=? WHERE id=? AND round_id=? AND state='accepted' AND payload_json=? AND report_json=? AND assignment_id=? AND assignment_epoch=?", ["accepted", job.id as string, round.id as string, job.payload_json as string, job.report_json as string, job.assignment_id as string, job.assignment_epoch as number]),
      write("UPDATE actions SET revision=?,snapshot_json=?,outcome_json=? WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS ? AND outcome_projection_id IS ?", [next.revision, { json: encodeActionSnapshot(observationBindings.length ? { ...next, snapshotFormat: "action-fields-v1", outcome: next.outcome ? { kind: next.outcome.kind } : null, latestObservation: { id: observationID } } : next) }, actionRow.outcome_projection_id != null ? actionRow.outcome_json as string : next.outcome ? { json: adopt ? storedOutcome : parseJson(actionRow.outcome_json as string) } : null, action.id, action.revision, actionRow.snapshot_json as string, actionRow.outcome_json as string | null, actionRow.outcome_projection_id as string | null]),
      write("UPDATE work_items SET stage=? WHERE id=? AND kind='action' AND subject_id=? AND stage=?", [adopt ? "done" : previousStage, work.id as string, action.id, work.stage as string]),
      write("UPDATE work_items SET stage=? WHERE id=? AND kind='operator' AND stage='pending'", ["done", control.id as string]),
      write("INSERT INTO action_observations(id,action_id,attempt_id,source,source_id,snapshot_json) VALUES(?,?,?,?,?,?)", [observationID, action.id, observation.attemptID, observation.source, legacy ? legacy.row.id as string : id, { json: storedObservation }])];
    if (legacy) writes.unshift(write("UPDATE action_attempt_state SET execute_dispatched=execute_dispatched+0 WHERE action_id=? AND attempt_id=? AND report_value_limit IS NULL", [action.id, legacy.row.attempt_id as string]),
      write("UPDATE report_processing SET state='done',revision=revision+1 WHERE report_id=? AND state='deferred' AND revision=?", [legacy.row.id as string, legacy.row.processing_revision as number]));
    if (knownEffect) writes.unshift(knownEffect);
    for (const [ordinal, sample] of observation.measurements.entries()) writes.push(write("INSERT INTO action_measurements(observation_id,ordinal,sample_json) VALUES(?,?,?)", [observationID, ordinal, { json: json(sample) }]));
    const events = [{ type: "action.observed", subjectID: action.id, data: observationBindings.length ? commandProjection(source, { actionID: action.id, observation }, observationBindings.map(binding => ({ ...binding, path: ["observation", ...binding.path] }))) : json({ actionID: action.id, observation }) }, { type: "resolution.applied", subjectID: action.id, data: json({ principalID: principal, requestKey: request.requestKey, observationID }) }];
    if (adopt) {
      const factID = randomUUID();
      writes.unshift(cancellationGuard({ kind: "action", id: action.id }), write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND business_cancelled=0 AND business_terminal=0", [run.id]));
      writes.push(write("INSERT INTO facts(id,run_id,action_id,outcome_json) VALUES(?,?,?,?)", [factID, run.id, action.id, { json: storedOutcome }]));
      events.push({ type: "action.settled", subjectID: action.id, data: source && validation.valid ? commandProjection(source, { actionID: action.id, outcome, sourceObservationID: observationID, settledAt: at }, [{ path: ["outcome"], selector: "result" }]) : json({ actionID: action.id, outcome, sourceObservationID: observationID, settledAt: at }) });
    }
    options.authorizeOperator(principal, runRow.principal_id as string, action);
    store.commit({ operationID: `${id}:apply`, kind: "action.resolve_applied", subjectID: action.id, request: parseJson(row.request_json as string), writes, events, result: storedReceipt, evidence: json({ observationID }), validBefore: round.deadline_at as number });
  }
  async function scan() {
    if (closed || scanning) return;
    scanning = true;
    try {
      store.assertAuthority();
      await retireCommandValues(store);
      const rows = store.query(`SELECT * FROM commands WHERE operation='resolveAction' AND status='pending'${cursor ? " AND (created_at,principal_id,request_key)>(?,?,?)" : ""} ORDER BY created_at,principal_id,request_key LIMIT 16`, cursor ?? []);
      for (const row of rows) try { await process(row); } catch { /* Uncertain work remains pending. */ }
      const last = rows.at(-1);
      cursor = rows.length === 16 && last ? [last.created_at as number, last.principal_id as string, last.request_key as string] : undefined;
    } catch { /* Fenced storage cannot process commands. */ } finally { scanning = false; }
  }
  return {
    async resolveAction(request: ResolveActionRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant = () => principal, fragmentedResponses = false): Promise<ActionView> {
      let pending = false;
      let checkedRequest: string | undefined;
      for (;;) {
        let prior: Row | undefined;
        try {
          if (closed) throw harnessError("INTERNAL_ERROR", "Coordinator stopped");
          store.assertAuthority();
          const grant = revalidate(); authorizeOperation(grant, "admin.resolveAction");
          prior = command(principal.principalID, request.requestKey);
          authorize(prior?.operation === "resolveAction" ? commandRequestMetadata(prior.request_json).actionID : request.actionID, grant);
        } catch (error) {
          if (pending) throw harnessError("INTERNAL_ERROR", "Operator command remains pending; query the original request key", null, "uncertain");
          throw error;
        }
        if (prior) {
          if (prior.operation !== "resolveAction") throw harnessError("REQUEST_CONFLICT", "Request key is bound to another command");
          if (prior.request_json !== checkedRequest) {
            const source = commandSource(prior.request_json);
            if (source) {
              verifyCommandValue(store, source);
              try { await verifyCanonicalValue(store, source.value, request); } catch (error) { if (error instanceof HarnessError && error.info.code === "INVALID_ARGUMENT") throw harnessError("REQUEST_CONFLICT", "Request key is bound to another command"); throw error; }
              const fresh = revalidate(); if (fresh.principalID !== principal.principalID) throw harnessError("FORBIDDEN", "Operator identity changed"); authorizeOperation(fresh, "admin.resolveAction"); authorize(source.actionID, fresh);
            } else if (!sameCommand(readCommandRequest(store, prior.request_json), request)) throw harnessError("REQUEST_CONFLICT", "Request key is bound to another command");
            checkedRequest = prior.request_json as string;
          }
          const settled = receipt(prior); if (settled) return settled;
        } else {
          checkRequestValues(request);
          try { await register(request, principal.principalID, fragmentedResponses, revalidate); } catch (error) { if (error instanceof HarnessError && error.info.disposition !== "uncertain" && ["REVISION_CONFLICT", "REQUEST_CONFLICT", "EXECUTION_FENCED"].includes(error.info.code)) continue; throw error; }
        }
        pending = true;
        await scan();
        await new Promise<void>(resolve => setTimeout(resolve, options.pollMs));
      }
    },
    start() { if (!timer && !closed) { void scan(); timer = setInterval(() => { void scan(); }, options.pollMs); timer.unref(); } },
    close() { closed = true; if (timer) clearInterval(timer); },
  };
}
