import { readRunSnapshot } from "../persistence/index.js";
import { epochValuePublicationWrites } from "../persistence/index.js";
import { encodeActionSnapshot, readActionExecutionState, type ActionExecutionState } from "../persistence/index.js";
﻿import { createHash, randomUUID } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, copyJson, harnessError, HarnessError, parseJson, validateReport, type ActionObservation, type ActionView, type CapacityResult, type Checkpoint, type Issue, type Json, type PreparedActionRequest, type RunView } from "../contract/index.js";
import { ValueStore, ReportTransferBindings, cancellationGuard, inspectVerifiedReport, readActionControl, readInlineActionSnapshot, reportValueOwner, verifyCanonicalValue, verifyDirectDiagnostic, type VerifiedReport, type ValuePublication, type SQLiteStore, type SQLWrite, type HistoryEvent } from "../persistence/index.js";
import { BusinessLeaseService, type LeaseGrant } from "./lease.js";
import { abandonCheckpointValue, checkpointHasLargeValue, checkpointPublicationWrites, checkpointWithSource, checkpointValueByteDelta, encodeCheckpoint, readCheckpointValue, stageCheckpointValue, type CheckpointValueSource } from "../persistence/index.js";

type Row = ReturnType<SQLiteStore["query"]>[number];
const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
const id = (value: unknown): value is string => typeof value === "string" && value.length > 0 && value.trim() === value;
const identity = (kind: string, parts: string[]) => {
  const hash = createHash("sha256");
  for (const chunk of canonicalJsonChunks(parts)) hash.update(chunk);
  return `action.${kind}:${hash.digest("hex")}`;
};
export type ActionPhase = "execute" | "reconcile";
export type ActionReport = { kind: "report"; value: Json } | { kind: "diagnostic"; code: string; phase: ActionPhase; digest?: string };
type StagedReportSource = { grant: LeaseGrant; actionID: string; attemptID: string; phase: ActionPhase; assignmentID: string };
type ReportTransfer = { id: string; sourceDigest: string; workerDigest: string };
// Both variants acknowledge durable handling of this original report identity.
// `diagnostic` retires sender retries; it promises neither saved raw bytes nor
// a successful Action. Failed/unknown commits must throw instead of returning it.
type StagedReportOutcome = { stored: true } | { stored: false; diagnostic: true };
export interface PreparedActionClaim {
  runID: string; rootID: string; principalID: string; request: PreparedActionRequest; checkpoint: Checkpoint;
  mode: "automatic-recovery" | "recheck-only"; recovering: boolean; stage: ActionPhase; attemptID: string; attemptNumber: number; reportValueLimit: number;
  writes: SQLWrite[]; events: HistoryEvent[];
}

/** Measure the complete single-item projections of an Action receiving a report. */
export function reportResponseBytes(domainID: string, principalID: string, action: ActionView,
  input: { attemptID: string; phase: ActionPhase }, reported: Json, includeFailureProjection = false): number {
  let result: CapacityResult | null = null, evidence: Json = null, measurements: ActionObservation["measurements"] = [], unresolved: Issue | null = null;
  try {
    const parsed = input.phase === "execute" ? validateReport(reported, "execute") : validateReport(reported, "reconcile");
    result = input.phase === "execute" ? parsed.payload as CapacityResult : parsed.payload.kind === "settled" ? parsed.payload.result : null;
    evidence = parsed.evidence; measurements = parsed.measurements;
    if (input.phase === "reconcile" && parsed.payload.kind === "unresolved") unresolved = parsed.payload.reason;
  } catch { /* Invalid reports still retain their raw report in the observation. */ }
  const max = Number.MAX_SAFE_INTEGER, observation: ActionObservation = { id: "00000000-0000-0000-0000-000000000000", attemptID: input.attemptID, source: input.phase,
    observedAt: max, result, evidence, validationError: null, measurements, reported, validated: false, acceptedAsOutcome: false };
  const next: ActionView = { ...action, revision: max, updatedAt: max, latestObservation: observation,
    ...(result && !action.outcome ? { status: "settled" as const, outcome: result, outcomeObservationID: observation.id, settledAt: max } : {}),
    ...(action.latestAttempt ? { latestAttempt: { ...action.latestAttempt, finishedAt: max } } : {}) };
  const envelope = (value: unknown) => ({ protocolVersion: "harness/1", requestID: "00000000-0000-0000-0000-000000000000", binding: { domainID, principalID }, ok: true, result: value });
  const cursor = Buffer.from(canonicalJson({ high: max, position: max })).toString("base64url") + "." + "A".repeat(43);
  const error = { code: "RESULT_CONTRACT_VIOLATION", message: "Capacity report is unavailable or invalid", details: { code: "RESULT_CONTRACT_VIOLATION" } };
  const failureObservation = { ...observation, validationError: error };
  const failureNext: ActionView = { ...next, latestObservation: failureObservation,
    ...(!action.outcome ? { outcome: { kind: "failed" as const, error }, blocked: error } : {}) };
  const projections = includeFailureProjection ? [next, failureNext, ...(unresolved ? [{ ...next, blocked: unresolved }] : [])] : [next];
  return Math.max(...projections.flatMap(view => {
    const history = { id: "00000000-0000-0000-0000-000000000000", sequence: max, at: max, runID: action.runID, actionID: action.id,
      attemptID: input.attemptID, type: "action.observed", data: { actionID: action.id, observation: view.latestObservation } };
    return [envelope(view), envelope({ items: [view], nextCursor: cursor }), envelope({ items: [history], nextCursor: cursor })]
      .map(value => Buffer.byteLength(canonicalJson(value), "utf8"));
  }));
}

/** Private managed Capacity operations. Report receipt is not settlement. */
export class ActionService {
  readonly leases: BusinessLeaseService;
  constructor(private readonly store: SQLiteStore, private readonly options: { clock: { now(): number }; maxValueBytes: number; maxPayloadBytes: number; maxResponseBytes?: number; valueQuota?: number; canDispatchRun(runID: string): boolean; authorizeRecheck?: (actionID: string) => SQLWrite; authorizeRetry?: (actionID: string) => SQLWrite }) {
    if (!Number.isSafeInteger(options.maxValueBytes) || options.maxValueBytes <= 0 || !Number.isSafeInteger(options.maxPayloadBytes) || options.maxPayloadBytes <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid Action value limit");
    this.leases = new BusinessLeaseService(store, { clock: options.clock, ttlMs: 1, authorizeRecheck: options.authorizeRecheck });
  }

  /** Append to the exact node.claim transaction after prepareClaim's lease writes. */
  prepareClaim(grant: LeaseGrant): PreparedActionClaim {
    if (grant.kind !== "action" || !id(grant.attemptID) || !Number.isSafeInteger(grant.attemptNumber) || grant.attemptNumber! < 1) throw harnessError("INVALID_ARGUMENT", "Action claim requires an Attempt grant");
    // Reject ineligible work from its bound control record before loading the
    // complete Action snapshot or any report-derived business value.
    const control = readActionControl(this.store, grant.subjectID);
    const pending = this.store.query("SELECT stage FROM work_items WHERE kind='action' AND subject_id=?", [grant.subjectID])[0];
    const controlRecheck = pending?.stage === "recheck" && control.recheckRequested || pending?.stage === "running" && control.latestAttempt?.mode === "recheck-only";
    if (control.kind !== "capacity" || pending && !controlRecheck && (control.outcomeKind !== null || control.cancelRequested || control.blockedCode !== null || !["accepted", "running"].includes(pending.stage as string)))
      throw harnessError("EXECUTION_FENCED", "Action is not eligible for work");
    const { action, run, actionRow, runRow, work } = this.current(grant.subjectID);
    const recheck = work.stage === "recheck" && action.recheckRequested || work.stage === "running" && action.latestAttempt?.mode === "recheck-only";
    if (action.request.kind !== "capacity" || (!recheck && (action.outcome || action.cancelRequested || action.blocked || run.outcome || run.cancelRequested || !["accepted", "running"].includes(work.stage as string)))) throw harnessError("EXECUTION_FENCED", "Action is not eligible for work");
    if (!recheck && (!this.options.canDispatchRun(run.id) || !["started", "resumed", "suspended"].includes(run.lifecycle ?? "") || !runRow.core_json)) throw harnessError("EXECUTION_FENCED", "Run control transition is not stable");
    const prior = this.store.query("SELECT attempt_id FROM action_attempt_state WHERE action_id=? AND execute_dispatched=1 LIMIT 1", [action.id]);
    const retry = !recheck ? this.store.query("SELECT * FROM work_items WHERE id=? AND kind='operator_retry' AND stage IN ('pending','granted')", [`operator.retry:${action.id}`])[0] : undefined;
    const retriedAttempt = retry?.result_json ? decode<{ attemptID: string }>(retry.result_json) : null;
    const retryReady = !!retry && (retry.stage === "pending" || retry.stage === "granted" && retriedAttempt?.attemptID === action.latestAttempt?.id && action.latestAttempt?.mode === "automatic-recovery" && !action.latestAttempt.executeDispatched);
    if (retryReady && !this.options.authorizeRetry) throw harnessError("EXECUTION_FENCED", "Operator retry authority is unavailable");
    const recovering = action.latestAttempt !== null;
    const stage: ActionPhase = recheck || prior.length && !retryReady ? "reconcile" : "execute";
    const mode = recheck ? "recheck-only" as const : "automatic-recovery" as const;
    const at = this.now();
    const attempt = { id: grant.attemptID, number: grant.attemptNumber!, nodeID: grant.nodeID, recovering, mode, executeDispatched: false, startedAt: at, finishedAt: null };
    const updated: ActionView | ActionExecutionState = { ...action, status: action.outcome ? action.status : "running", recheckRequested: false, latestAttempt: attempt, revision: action.revision + 1, updatedAt: at };
    const writes = [
      write(`UPDATE runs SET epoch=epoch+0 WHERE id=? AND revision=? AND snapshot_json=? AND core_json IS ?${recheck ? "" : " AND business_cancelled=0 AND business_terminal=0"}`, [run.id, run.revision, runRow.snapshot_json as string, runRow.core_json as string | null]),
      write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["running", work.id as string, "action", action.id, work.stage as string]),
      write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND run_id=? AND revision=? AND request_json=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS ?", [updated.revision, { json: encodeActionSnapshot(updated) }, action.id, run.id, action.revision, actionRow.request_json as string, actionRow.snapshot_json as string, actionRow.outcome_json as string | null]),
      write("INSERT INTO action_attempt_state(attempt_id,action_id,mode,recovering,stage,execute_dispatched,dispatch_request_id,report_value_limit) VALUES(?,?,?,?,?,?,?,?)", [attempt.id, action.id, mode, recovering ? 1 : 0, stage, 0, null, this.options.maxValueBytes]),
    ];
    if (retryReady && retry) {
      const payload = decode<{ reportID: string }>(retry.payload_json);
      if (!id(payload.reportID)) throw harnessError("EXECUTION_FENCED", "Retry evidence is unavailable");
      const previous = this.store.query("SELECT * FROM action_retry_authorizations WHERE action_id=?", [action.id])[0];
      writes.push(this.options.authorizeRetry!(action.id),
        write("UPDATE work_items SET stage=?,result_json=? WHERE id=? AND kind='operator_retry' AND subject_id=? AND stage=? AND payload_json=? AND result_json IS ?", ["granted", { json: json({ attemptID: attempt.id }) }, retry.id as string, action.id, retry.stage as string, retry.payload_json as string, retry.result_json as string | null]),
        previous ? write("UPDATE action_retry_authorizations SET report_id=?,attempt_id=?,consumed=0 WHERE action_id=? AND report_id=? AND attempt_id=? AND consumed=?", [payload.reportID, attempt.id, action.id, previous.report_id as string, previous.attempt_id as string, previous.consumed as number])
          : write("INSERT INTO action_retry_authorizations(action_id,report_id,attempt_id,consumed) VALUES(?,?,?,0)", [action.id, payload.reportID, attempt.id]));
    }
    const events = [{ type: "attempt.started", subjectID: action.id, data: json({ runID: run.id, actionID: action.id, attemptID: attempt.id, number: attempt.number, nodeID: grant.nodeID, mode, recovering, stage }) }];
    return { runID: run.id, rootID: run.rootID, principalID: runRow.principal_id as string, request: action.request, checkpoint: action.checkpoint, mode, recovering, stage, attemptID: attempt.id, attemptNumber: attempt.number, reportValueLimit: this.options.maxValueBytes, writes, events };
  }

  dispatch(input: { grant: LeaseGrant; actionID: string; attemptID: string; dispatchRequestID: string }): { executionCount: number } {
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    if (!id(input.dispatchRequestID)) throw harnessError("INVALID_ARGUMENT", "Invalid execute intent identity");
    const operationID = identity("dispatch", [input.attemptID, input.dispatchRequestID]);
    const prior = this.replay(operationID, "action.execute_intent", input.actionID, input);
    this.leases.assert(input.grant);
    if (prior.found) {
      if ((prior.result as { blocked?: boolean } | null)?.blocked) throw harnessError("LIMIT_EXCEEDED", "Action execution budget exhausted");
      return prior.result as unknown as { executionCount: number };
    }
    const { action, run, actionRow, runRow, work } = this.current(input.actionID);
    this.assertRunning(input.grant, action, run, work);
    const attempt = this.store.query("SELECT * FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [input.attemptID, input.actionID])[0];
    if (!attempt || attempt.stage !== "execute" || attempt.mode !== "automatic-recovery" || attempt.execute_dispatched !== 0 || action.latestAttempt!.executeDispatched) throw harnessError("EXECUTION_FENCED", "Execute intent is unavailable");
    const priorExecution = this.store.query("SELECT attempt_id FROM action_attempt_state WHERE action_id=? AND execute_dispatched=1 LIMIT 1", [action.id])[0];
    const retryAuthorization = priorExecution && this.store.query("SELECT report_id FROM action_retry_authorizations WHERE action_id=? AND attempt_id=? AND consumed=0", [action.id, input.attemptID])[0];
    if (priorExecution && !retryAuthorization) throw harnessError("RECONCILIATION_REQUIRED", "A safe retry has not been authorized");
    const operatorRetry = retryAuthorization && this.store.query("SELECT * FROM work_items WHERE id=? AND kind='operator_retry' AND stage='granted'", [`operator.retry:${action.id}`])[0];
    let operatorGuards: SQLWrite[] = [];
    if (operatorRetry) {
      const payload = decode<{ reportID: string }>(operatorRetry.payload_json), allocated = decode<{ attemptID: string }>(operatorRetry.result_json);
      if (payload.reportID !== retryAuthorization!.report_id || allocated.attemptID !== input.attemptID || !this.options.authorizeRetry) throw harnessError("EXECUTION_FENCED", "Operator retry identity changed");
      operatorGuards = [this.options.authorizeRetry(action.id), write("UPDATE work_items SET stage=? WHERE id=? AND kind='operator_retry' AND stage='granted' AND payload_json=? AND result_json=?", ["done", operatorRetry.id as string, operatorRetry.payload_json as string, operatorRetry.result_json as string])];
    }
    if (action.executionCount >= run.limits.maxExecutionsPerAction) {
      const blocked: ActionView | ActionExecutionState = { ...action, revision: action.revision + 1, updatedAt: this.now(), blocked: { code: "LIMIT_EXCEEDED", message: "Action execution budget exhausted", details: { maxExecutionsPerAction: run.limits.maxExecutionsPerAction } } };
      this.store.commit({ operationID, kind: "action.execute_intent", subjectID: action.id, request: json(input), evidence: json({ blocked: blocked.blocked }), result: { blocked: true }, validBefore: input.grant.expiresAt,
        writes: [...this.leases.guard(input.grant), write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [run.id, run.revision, runRow.snapshot_json as string]), write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["blocked", work.id as string, "action", action.id, "running"]),
          write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND run_id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS NULL", [blocked.revision, { json: encodeActionSnapshot(blocked) }, action.id, run.id, action.revision, actionRow.snapshot_json as string])],
        events: [{ type: "action.blocked", subjectID: action.id, data: json({ actionID: action.id, attemptID: input.attemptID, blocked: blocked.blocked }) }] });
      throw harnessError("LIMIT_EXCEEDED", "Action execution budget exhausted");
    }
    const at = this.now();
    const updated: ActionView | ActionExecutionState = { ...action, revision: action.revision + 1, updatedAt: at, executionCount: action.executionCount + 1, latestAttempt: { ...action.latestAttempt!, executeDispatched: true } };
    const result = { executionCount: updated.executionCount };
    const receipt = this.store.commit({ operationID, kind: "action.execute_intent", subjectID: action.id, request: json(input), result: json(result), evidence: json(result), validBefore: input.grant.expiresAt,
      writes: [...this.leases.guard(input.grant), ...operatorGuards, write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [run.id, run.revision, runRow.snapshot_json as string]), this.workGuard(work, action.id),
        ...(retryAuthorization ? [write("UPDATE action_retry_authorizations SET consumed=1 WHERE action_id=? AND attempt_id=? AND report_id=? AND consumed=0", [action.id, input.attemptID, retryAuthorization.report_id as string])] : []),
        write("UPDATE action_attempt_state SET execute_dispatched=1,dispatch_request_id=? WHERE attempt_id=? AND action_id=? AND stage=? AND mode=? AND execute_dispatched=0", [input.dispatchRequestID, input.attemptID, action.id, "execute", "automatic-recovery"]),
        write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND run_id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS NULL", [updated.revision, { json: encodeActionSnapshot(updated) }, action.id, run.id, action.revision, actionRow.snapshot_json as string])],
      events: [{ type: "attempt.execute_dispatched", subjectID: action.id, data: json({ actionID: action.id, attemptID: input.attemptID, executionCount: result.executionCount }) }] });
    return receipt.result as unknown as typeof result;
  }

  checkpoint(input: { grant: LeaseGrant; actionID: string; attemptID: string; operationID: string; expectedRevision: number; value: Json }, assertCaller: () => void = () => {}): Checkpoint | Promise<Checkpoint> {
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    if (!id(input.operationID) || !Number.isSafeInteger(input.expectedRevision) || input.expectedRevision < 0) throw harnessError("INVALID_ARGUMENT", "Invalid checkpoint operation");
    const value = json(input.value);
    const operationID = identity("checkpoint", [input.attemptID, input.operationID]);
    const prior = this.checkpointReplay(input);
    this.leases.assert(input.grant);
    const { action, run, actionRow, work } = this.current(input.actionID);
    this.assertRunning(input.grant, action, run, work);
    if (prior) return prior;
    if (checkpointHasLargeValue(value)) return this.largeCheckpoint(input, value, assertCaller).catch(error => {
      // Two deliveries may stage the same save concurrently. A changed
      // revision can be the original operation winning, never a new save.
      if (error instanceof HarnessError && error.info.code === "REVISION_CONFLICT" && error.info.disposition === "rejected") {
        assertCaller(); this.leases.assert(input.grant);
        const replay = this.checkpointReplay(input);
        if (replay) { const current = this.current(input.actionID); this.assertRunning(input.grant, current.action, current.run, current.work); return replay; }
      }
      throw error;
    });
    if (Buffer.byteLength(canonicalJson(value), "utf8") > this.options.maxValueBytes) throw harnessError("LIMIT_EXCEEDED", "Checkpoint exceeds the value limit");
    if (action.checkpoint.revision !== input.expectedRevision || !Number.isSafeInteger(input.expectedRevision + 1)) throw harnessError("REVISION_CONFLICT", "Checkpoint revision changed");
    const checkpoint = { revision: input.expectedRevision + 1, value };
    const updated: ActionView | ActionExecutionState = { ...action, checkpoint, revision: action.revision + 1, updatedAt: this.now() };
    const receipt = this.store.commit({ operationID, kind: "action.checkpoint", subjectID: action.id, request: json(input), evidence: json(checkpoint), result: json(checkpoint), validBefore: input.grant.expiresAt,
      writes: [...this.leases.guard(input.grant), this.workGuard(work, action.id), write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND run_id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS NULL", [updated.revision, { json: encodeActionSnapshot(updated) }, action.id, run.id, action.revision, actionRow.snapshot_json as string])],
      events: [{ type: "checkpoint.committed", subjectID: action.id, data: json({ actionID: action.id, attemptID: input.attemptID, checkpoint }) }] });
    return receipt.result as unknown as Checkpoint;
  }

  /** An immutable save is compared by its complete original native content;
   * neither its value digest nor a staged value is a successful save receipt. */
  checkpointReplay(input: { grant: LeaseGrant; actionID: string; attemptID: string; operationID: string; expectedRevision: number; value: Json }): Checkpoint | null {
    const row = this.store.query("SELECT type,subject_id,request_json,result_json FROM history WHERE operation_id=?", [identity("checkpoint", [input.attemptID, input.operationID])])[0];
    if (!row) return null;
    const stored = decode<Record<string, Json>>(row.request_json);
    const checkpoint = readCheckpointValue(this.store, input.actionID, parseJson(row.result_json as string));
    const { valueSource: _source, ...metadata } = stored;
    const original = { ...metadata, value: checkpoint.value };
    const left = canonicalJsonChunks(original)[Symbol.iterator](), right = canonicalJsonChunks(input)[Symbol.iterator]();
    let same = true;
    for (;;) { const a = left.next(), b = right.next(); if (a.done || b.done) { same = a.done === b.done; break; } if (!Buffer.from(a.value).equals(b.value)) { same = false; break; } }
    if (row.type !== "action.checkpoint" || row.subject_id !== input.actionID || !same) throw harnessError("REQUEST_CONFLICT", "Action operation identity was reused");
    return checkpoint;
  }

  private async largeCheckpoint(input: { grant: LeaseGrant; actionID: string; attemptID: string; operationID: string; expectedRevision: number; value: Json }, value: Json, assertCaller: () => void): Promise<Checkpoint> {
    let bytes = 0;
    // Chunk checks read control metadata only. Rebuilding the previous complete
    // checkpoint on every chunk would amplify work and delay lease renewal.
    const check = () => {
      assertCaller(); this.leases.assert(input.grant);
      const action = readActionControl(this.store, input.actionID);
      const row = this.store.query(`SELECT a.snapshot_projection_id,json_extract(a.snapshot_json,'$.checkpoint.revision') checkpoint_revision,w.stage,
        r.business_cancelled run_cancelled,r.business_terminal run_terminal FROM actions a JOIN runs r ON r.id=a.run_id
        JOIN work_items w ON w.kind='action' AND w.subject_id=a.id WHERE a.id=?`, [input.actionID])[0];
      if (!row || row.snapshot_projection_id !== null || row.stage !== "running" || action.outcomeKind !== null || action.cancelRequested || row.run_cancelled !== 0 || row.run_terminal !== 0
          || action.latestAttempt?.id !== input.attemptID || action.latestAttempt.number !== input.grant.attemptNumber || !this.options.canDispatchRun(action.runID)) throw harnessError("EXECUTION_FENCED", "Action is no longer eligible for checkpoint publication");
      if (row.checkpoint_revision !== input.expectedRevision || !Number.isSafeInteger(input.expectedRevision + 1)) throw harnessError("REVISION_CONFLICT", "Checkpoint revision changed");
    };
    for (const chunk of canonicalJsonChunks(value)) { check(); bytes += chunk.length; if (bytes > this.options.maxValueBytes) throw harnessError("LIMIT_EXCEEDED", "Checkpoint exceeds the value limit"); await new Promise<void>(resolve => setImmediate(resolve)); }
    check();
    let source: CheckpointValueSource | undefined;
    try {
      source = await stageCheckpointValue(this.store, { actionID: input.actionID, attemptID: input.attemptID, operationID: input.operationID, revision: input.expectedRevision + 1 }, value, this.options.valueQuota ?? Number.MAX_SAFE_INTEGER, check);
      check(); const { action, run, actionRow, work } = this.current(input.actionID); this.assertRunning(input.grant, action, run, work);
      const checkpoint = checkpointWithSource(source, value), encoded = encodeCheckpoint(checkpoint);
      const updated: ActionView | ActionExecutionState = { ...action, checkpoint, revision: action.revision + 1, updatedAt: this.now() };
      this.store.commit({ operationID: identity("checkpoint", [input.attemptID, input.operationID]), kind: "action.checkpoint", subjectID: action.id,
        request: json({ ...input, value: null, valueSource: source }), evidence: encoded, result: encoded, validBefore: input.grant.expiresAt, valuePublications: [source.value],
        writes: [...this.leases.guard(input.grant), this.workGuard(work, action.id), ...checkpointPublicationWrites(source, this.store.coordinatorEpoch),
          write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND run_id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS NULL", [updated.revision, { json: encodeActionSnapshot(updated) }, action.id, run.id, action.revision, actionRow.snapshot_json as string])],
        events: [{ type: "checkpoint.committed", subjectID: action.id, data: json({ actionID: action.id, attemptID: input.attemptID, checkpoint: encoded }) }] });
      return checkpoint;
    } catch (error) { if (source) abandonCheckpointValue(this.store, source); throw error; }
  }

  /** Read an original receipt without restoring or extending old execution eligibility. */
  readCheckpoint(input: { grant: LeaseGrant; actionID: string; attemptID: string; operationID: string }): Checkpoint | null {
    const encoded = this.readCheckpointReceipt(input);
    return encoded === null ? null : readCheckpointValue(this.store, input.actionID, encoded);
  }
  /** Repeated fragment authorization verifies provenance without hydrating the
   * same complete checkpoint again. This never extends execution authority. */
  readCheckpointReceipt(input: { grant: LeaseGrant; actionID: string; attemptID: string; operationID: string }): Json | null {
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    if (!id(input.operationID)) throw harnessError("INVALID_ARGUMENT", "Invalid checkpoint identity");
    this.assertSource(input.grant, input.actionID, input.attemptID);
    const record = this.store.query("SELECT type,subject_id,request_json,result_json FROM history WHERE operation_id=?", [identity("checkpoint", [input.attemptID, input.operationID])])[0];
    if (!record) return null;
    const request = decode<{ grant: LeaseGrant; actionID: string; attemptID: string; operationID: string }>(record.request_json);
    if (record.type !== "action.checkpoint" || record.subject_id !== input.actionID || request.actionID !== input.actionID || request.attemptID !== input.attemptID || request.operationID !== input.operationID || request.grant.epoch !== input.grant.epoch || request.grant.incarnation !== input.grant.incarnation || request.grant.workerPrincipalID !== input.grant.workerPrincipalID) throw harnessError("FORBIDDEN", "Checkpoint receipt source does not match");
    const encoded = parseJson(record.result_json as string); checkpointValueByteDelta(this.store, input.actionID, encoded); return encoded;
  }

  recordReport(input: { grant: LeaseGrant; actionID: string; attemptID: string; phase: ActionPhase; assignmentID: string; report: ActionReport }, businessAuthorized = true): { stored: true } {
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    if (input.assignmentID !== input.attemptID || !["execute", "reconcile"].includes(input.phase)) throw harnessError("INVALID_ARGUMENT", "Invalid report association");
    if (!input.report || input.report.kind !== "report" && input.report.kind !== "diagnostic" || input.report.kind === "diagnostic" && (input.report.phase !== input.phase || !["DEPENDENCY_CREATE_FAILED", "POLICY_FAILED", "CAPACITY_THROWN", "RESULT_CONTRACT_VIOLATION"].includes(input.report.code) || input.report.digest !== undefined && (!id(input.report.digest) || input.report.digest.length > 128))) throw harnessError("INVALID_ARGUMENT", "Invalid report envelope");
    this.assertSource(input.grant, input.actionID, input.attemptID);
    let report = json(input.report);
    const reportDigest = createHash("sha256").update(canonicalJson(report)).digest("hex");
    const body = input.report.kind === "report" && input.report.value && typeof input.report.value === "object" && !Array.isArray(input.report.value) ? input.report.value : null;
    const operationID = identity("report", [input.actionID, input.attemptID, input.phase]);
    const previous = this.store.query("SELECT request_json FROM history WHERE operation_id=?", [operationID])[0];
    const previousDigest = previous ? decode<{ reportDigest?: string }>(previous.request_json).reportDigest : undefined;
    const attemptLimit = this.store.query("SELECT report_value_limit FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [input.attemptID, input.actionID])[0];
    if (!attemptLimit) throw harnessError("EXECUTION_FENCED", "Report Attempt state is unavailable");
    const unknownLimit = !previous && input.report.kind === "report" && attemptLimit.report_value_limit === null;
    const valueLimit = attemptLimit.report_value_limit as number | null;
    const oversized = previous ? previousDigest === reportDigest : !unknownLimit && input.report.kind === "report"
      && (Buffer.byteLength(canonicalJson(input.report.value), "utf8") > valueLimit!
        || !!body && [body.payload, body.evidence].some(value => value !== undefined && Buffer.byteLength(canonicalJson(value), "utf8") > valueLimit!));
    if (oversized) report = json({ kind: "diagnostic", code: "RESULT_CONTRACT_VIOLATION", phase: input.phase });
    // Report identity is source/content based; a confirmed lease renewal must
    // not turn an otherwise identical report into a different source.
    const request = { actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, assignmentID: input.assignmentID,
      source: { nodeID: input.grant.nodeID, incarnation: input.grant.incarnation, workerPrincipalID: input.grant.workerPrincipalID, epoch: input.grant.epoch }, report,
      ...(oversized ? { reportDigest } : {}) };
    let prior: { found: boolean; result: Json };
    try { prior = this.replay(operationID, "action.report_received", input.actionID, request); }
    catch (error) {
      if (!(error instanceof HarnessError) || error.info.code !== "REQUEST_CONFLICT") throw error;
      const conflictDigest = createHash("sha256").update(canonicalJson(request)).digest("hex");
      const conflictID = identity("report-conflict", [input.actionID, input.attemptID, input.phase, conflictDigest]);
      this.store.commit({ operationID: conflictID, kind: "action.report_conflict", subjectID: input.actionID,
        request: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, digest: conflictDigest }), writes: [],
        evidence: json({ conflict: true }), result: null,
        events: [{ type: "action.report_conflict", subjectID: input.actionID, data: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, digest: conflictDigest }) }] });
      throw error;
    }
    if (prior.found) return prior.result as unknown as { stored: true };
    const attempt = this.store.query("SELECT stage,execute_dispatched FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [input.attemptID, input.actionID])[0];
    if (!attempt || attempt.stage !== input.phase || input.phase === "execute" && attempt.execute_dispatched !== 1) throw harnessError("EXECUTION_FENCED", "Report does not correspond to a dispatched phase");
    const candidate = this.reportCurrent(input.actionID);
    const reportID = randomUUID(), receivedAt = this.now();
    const insertReport = (authority: LeaseGrant | null) => write("INSERT INTO action_reports(id,action_id,attempt_id,phase,assignment_id,report_json,authority_json,received_at) VALUES(?,?,?,?,?,?,?,?)", [reportID, input.actionID, input.attemptID, input.phase, input.assignmentID, { json: report }, authority ? { json: json(authority) } : null, receivedAt]);
    let current = false;
    if (businessAuthorized && candidate) try { this.leases.assert(input.grant); this.assertRunning(input.grant, candidate.action, candidate.run, candidate.work, true); current = true; } catch (error) {
      if (!this.reportEligibilityLost(error)) throw error;
    }
    const writes = [insertReport(current ? input.grant : null)];
    const quarantine = () => write("UPDATE report_processing SET state=? WHERE report_id=? AND state=?", ["deferred", reportID, "new"]);
    if (unknownLimit) writes.push(quarantine());
    if (current && candidate) writes.push(...this.leases.guard(input.grant), write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["report_pending", candidate.work.id as string, "action", candidate.action.id, "running"]));
    const batch = { operationID, kind: "action.report_received", subjectID: input.actionID, request: json(request), evidence: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase }), result: json({ stored: true }), writes,
      ...(unknownLimit ? { events: [{ type: "action.report_limit_unknown", subjectID: input.actionID, data: json({ actionID: input.actionID, attemptID: input.attemptID, reportID,
        reason: "legacy_attempt_limit_unavailable", phase: input.phase, reported: input.report.kind === "report" ? input.report.value : null }) }] } : {}),
      ...(current ? { validBefore: input.grant.expiresAt } : {}) };
    try { return this.store.commit(batch).result as unknown as { stored: true }; }
    catch (error) {
      if (!current || !this.definitelyFenced(error)) throw error;
      const committed = this.replay(operationID, "action.report_received", input.actionID, request);
      if (committed.found) return committed.result as unknown as { stored: true };
      // A lost race never makes the source authoritative. Preserve its report
      // without changing a newer owner's work, outcome or eligibility.
      this.assertSource(input.grant, input.actionID, input.attemptID);
      const { validBefore: _deadline, ...observationalBatch } = batch;
      return this.store.commit({ ...observationalBatch, writes: [insertReport(null), ...(unknownLimit ? [quarantine()] : [])] }).result as unknown as { stored: true };
    }
  }

  /** Canonical staged-report seam shared by ordinary reports and the private
   * transfer path. Publication still requires original source authorization. */
  async recordReportValue(input: { grant: LeaseGrant; actionID: string; attemptID: string; phase: ActionPhase; assignmentID: string; report: { kind: "report"; value: Json } }, ref: ValuePublication, reauthorize: () => boolean, transfer?: { id: string; sourceDigest: string; workerDigest: string }, currentGrant?: () => LeaseGrant): Promise<StagedReportOutcome> {
    input = copyJson(input) as unknown as typeof input;
    ref = copyJson(ref) as unknown as ValuePublication;
    if (transfer) transfer = copyJson(transfer) as unknown as typeof transfer;
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    if (input.assignmentID !== input.attemptID || !["execute", "reconcile"].includes(input.phase) || input.report?.kind !== "report"
        || ref.owner !== reportValueOwner(this.store.domainID, input.actionID, input.attemptID, input.phase))
      throw harnessError("INVALID_ARGUMENT", "Invalid staged report association");
    this.assertSource(input.grant, input.actionID, input.attemptID);
    reauthorize();
    const epoch = this.store.coordinatorEpoch, values = new ValueStore(this.store, Number.MAX_SAFE_INTEGER), currentValue = values.status(ref.id, ref.owner);
    if ((currentValue.state !== "sealed" && currentValue.state !== "published") || currentValue.encoding !== ref.encoding
        || currentValue.length !== ref.length || currentValue.digest !== ref.digest || currentValue.written !== ref.length)
      throw harnessError("INVALID_ARGUMENT", "Staged report manifest does not match");
    const check = () => { this.store.assertAuthority(); if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report staging authority changed"); reauthorize(); this.assertSource(input.grant, input.actionID, input.attemptID); };
    await verifyCanonicalValue(this.store, ref, input.report, check);
    const attemptLimit = this.store.query("SELECT report_value_limit FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [input.attemptID, input.actionID])[0];
    if (!attemptLimit) throw harnessError("EXECUTION_FENCED", "Report Attempt state is unavailable");
    const limit = attemptLimit.report_value_limit as number | null;
    const measure = async (value: Json) => { let count = 0; for (const part of canonicalJsonChunks(value)) { check(); count += part.byteLength; await new Promise<void>(resolve => setImmediate(resolve)); } check(); return count; };
    const body = input.report.value && typeof input.report.value === "object" && !Array.isArray(input.report.value) ? input.report.value : null;
    const valueBytes = await measure(input.report.value), payloadBytes = body?.payload === undefined ? null : await measure(body.payload), evidenceBytes = body?.evidence === undefined ? null : await measure(body.evidence);
    if (currentGrant) {
      const renewed = copyJson(currentGrant()) as unknown as LeaseGrant;
      if (canonicalJson({ ...renewed, expiresAt: input.grant.expiresAt }) !== canonicalJson(input.grant) || renewed.expiresAt < input.grant.expiresAt)
        throw harnessError("EXECUTION_FENCED", "Report renewal changed its original lease identity");
      input = { ...input, grant: renewed };
    }
    if (limit !== null && (valueBytes > limit || payloadBytes !== null && payloadBytes > limit || evidenceBytes !== null && evidenceBytes > limit)) {
      return this.publishStagedDiagnostic(input, ref, reauthorize, transfer, epoch, { limit, valueBytes, payloadBytes, evidenceBytes });
    }
    return this.publishStagedReport(input, ref, reauthorize, transfer, epoch, limit);
  }

  /** Admit bytes checked by the Persistence worker without reconstructing the
   * report on the coordinator thread. The token is process-private and binds
   * the exact store, epoch and immutable sealed publication. */
  async recordVerifiedReportValue(input: StagedReportSource, ref: ValuePublication, proof: VerifiedReport, reauthorize: () => boolean, transfer?: ReportTransfer): Promise<StagedReportOutcome> {
    input = copyJson(input) as unknown as StagedReportSource;
    ref = copyJson(ref) as unknown as ValuePublication;
    if (transfer) transfer = copyJson(transfer) as unknown as ReportTransfer;
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    if (input.assignmentID !== input.attemptID || !["execute", "reconcile"].includes(input.phase)
        || ref.owner !== reportValueOwner(this.store.domainID, input.actionID, input.attemptID, input.phase))
      throw harnessError("INVALID_ARGUMENT", "Invalid staged report association");
    this.assertSource(input.grant, input.actionID, input.attemptID);
    reauthorize();
    const epoch = this.store.coordinatorEpoch, lengths = inspectVerifiedReport(this.store, ref, proof);
    const attempt = this.store.query("SELECT report_value_limit FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [input.attemptID, input.actionID])[0];
    if (!attempt) throw harnessError("EXECUTION_FENCED", "Report Attempt state is unavailable");
    const limit = attempt.report_value_limit as number | null;
    const oversized = limit !== null && (lengths.valueBytes > limit || lengths.payloadBytes !== null && lengths.payloadBytes > limit
        || lengths.evidenceBytes !== null && lengths.evidenceBytes > limit);
    // Recheck the actual immutable manifest, not just a caller-provided digest.
    inspectVerifiedReport(this.store, ref, proof);
    if (oversized) {
      return this.publishStagedDiagnostic(input, ref, reauthorize, transfer, epoch, { limit: limit!, ...lengths });
    }
    return this.publishStagedReport(input, ref, reauthorize, transfer, epoch, limit);
  }

  /** A verified over-limit report becomes only finite diagnostic evidence.
   * Its receipt and any required same-Attempt block commit atomically with
   * retirement of the unreferenced staged bytes. */
  private async publishStagedDiagnostic(input: StagedReportSource, ref: ValuePublication, reauthorize: () => boolean,
    transfer: ReportTransfer | undefined, epoch: number, sizes: { limit: number; valueBytes: number; payloadBytes: number | null; evidenceBytes: number | null }): Promise<{ stored: false; diagnostic: true }> {
    if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report staging authority changed");
    const businessAuthorized = reauthorize(); this.assertSource(input.grant, input.actionID, input.attemptID);
    const values = new ValueStore(this.store, Number.MAX_SAFE_INTEGER), bindings = new ReportTransferBindings(this.store, values);
    const controlValue = transfer ? bindings.diagnosticControl(transfer, ref.id) : undefined;
    const operationID = identity("report", [input.actionID, input.attemptID, input.phase]);
    const reportID = createHash("sha256").update(operationID).digest("hex");
    const exceeded = (["value", "payload", "evidence"] as const).filter(key => {
      const bytes = sizes[`${key}Bytes`]; return bytes !== null && bytes > sizes.limit;
    });
    const report = { kind: "diagnostic", code: "RESULT_CONTRACT_VIOLATION", phase: input.phase, digest: ref.digest, ...sizes, exceeded };
    const request = { actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, assignmentID: input.assignmentID, reportID,
      source: { nodeID: input.grant.nodeID, incarnation: input.grant.incarnation, workerPrincipalID: input.grant.workerPrincipalID, epoch: input.grant.epoch },
      report, reportDigest: ref.digest, reportBytes: ref.length, reportDigestScope: "canonical-report-envelope-utf8-v1",
      ...(transfer ? { controlValue, transfer } : { reportOwner: ref.owner, reportEncoding: ref.encoding }) };
    const existing = this.replay(operationID, "action.report_received", input.actionID, request);
    if (existing.found) {
      if (!transfer) {
        const receipt = this.store.query("SELECT * FROM report_direct_diagnostics WHERE operation_id=?", [operationID])[0];
        if (!receipt || !verifyDirectDiagnostic(this.store, { operationID, kind: "action.report_received", subjectID: input.actionID, request: json(request), evidence: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase }), result: json({ stored: false, diagnostic: true }), writes: [] })
            || receipt.report_digest !== ref.digest || receipt.report_length !== ref.length || receipt.report_owner !== ref.owner || receipt.report_encoding !== ref.encoding
            || receipt.action_id !== input.actionID || receipt.attempt_id !== input.attemptID || receipt.phase !== input.phase || receipt.report_id !== reportID)
          throw harnessError("INTERNAL_ERROR", "Direct diagnostic receipt is inconsistent");
        const staged = values.status(ref.id, ref.owner);
        if (staged.state !== "sealed") throw harnessError("STATE_CONFLICT", "Replayed report is unavailable");
        await values.retireUnbound(staged as typeof staged & { state: "sealed" }, [write("UPDATE action_attempt_state SET execute_dispatched=execute_dispatched+0 WHERE attempt_id=? AND action_id=? AND stage=? AND report_value_limit=?", [input.attemptID, input.actionID, input.phase, sizes.limit])]);
        if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report staging authority changed");
        reauthorize(); this.assertSource(input.grant, input.actionID, input.attemptID);
      }
      return existing.result as unknown as { stored: false; diagnostic: true };
    }
    const attempt = this.store.query("SELECT stage,execute_dispatched FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [input.attemptID, input.actionID])[0];
    if (!attempt || attempt.stage !== input.phase || input.phase === "execute" && attempt.execute_dispatched !== 1)
      throw harnessError("EXECUTION_FENCED", "Report does not correspond to a dispatched phase");
    const actionRow = this.store.query("SELECT * FROM actions WHERE id=?", [input.actionID])[0];
    if (!actionRow) throw harnessError("NOT_FOUND", "Action not found");
    if (actionRow.snapshot_projection_id !== null && readActionControl(this.store, input.actionID).outcomeKind === null)
      throw harnessError("STATE_CONFLICT", "Referenced Action requires a supported diagnostic block path");
    let current: ReturnType<ActionService["current"]> | undefined;
    if (businessAuthorized && actionRow.snapshot_projection_id === null) {
      const candidate = this.current(input.actionID);
      try {
        this.leases.assert(input.grant); this.assertRunning(input.grant, candidate.action, candidate.run, candidate.work, true);
        if (!candidate.action.outcome && !candidate.action.cancelRequested && !candidate.action.blocked && !candidate.run.outcome && !candidate.run.cancelRequested
            && !this.store.effectiveCancellation({ kind: "action", id: input.actionID })) current = candidate;
      } catch (error) {
        if (!this.reportEligibilityLost(error)) throw error;
        // A definitively fenced Attempt remains observational.
      }
    }
    const receivedAt = this.now();
    const insert = () => write("INSERT INTO action_reports(id,action_id,attempt_id,phase,assignment_id,report_json,authority_json,received_at) VALUES(?,?,?,?,?,?,?,?)",
      [reportID, input.actionID, input.attemptID, input.phase, input.assignmentID, { json: json(report) }, null, receivedAt]);
    const staged = values.status(ref.id, ref.owner);
    if (staged.state !== "sealed") throw harnessError("STATE_CONFLICT", "Verified diagnostic report is unavailable");
    const retirement = transfer ? bindings.diagnosticWrites(transfer, ref, input.actionID, input.attemptID, input.phase, reportID, operationID) : [
      write("UPDATE value_manifests SET state=?,revision=? WHERE id=? AND owner=? AND state=? AND revision=? AND total_bytes=? AND digest=?",
        ["deleting", staged.revision + 1, ref.id, ref.owner, "sealed", staged.revision, ref.length, ref.digest]),
      write("INSERT INTO report_direct_diagnostics(operation_id,report_id,report_value_id,report_owner,report_encoding,report_length,report_digest,action_id,attempt_id,phase) VALUES(?,?,?,?,?,?,?,?,?,?)",
        [operationID, reportID, ref.id, ref.owner, ref.encoding, ref.length, ref.digest, input.actionID, input.attemptID, input.phase])];
    const common = [write("UPDATE action_attempt_state SET execute_dispatched=execute_dispatched+0 WHERE attempt_id=? AND action_id=? AND stage=? AND execute_dispatched=? AND report_value_limit=?",
      [input.attemptID, input.actionID, input.phase, attempt.execute_dispatched as number, sizes.limit]), insert(), ...retirement];
    const events: HistoryEvent[] = [];
    const writes = [...common];
    if (current) {
      const { action, run, work } = current;
      const blocked: ActionView | ActionExecutionState = { ...action, revision: action.revision + 1, updatedAt: receivedAt,
        blocked: { code: "RECONCILIATION_REQUIRED", message: "Capacity outcome requires reconciliation", details: { reportID, phase: input.phase, code: "RESULT_CONTRACT_VIOLATION" } },
        latestAttempt: action.latestAttempt?.id === input.attemptID ? { ...action.latestAttempt, finishedAt: receivedAt } : action.latestAttempt };
      writes.unshift(...this.leases.guard(input.grant), cancellationGuard({ kind: "action", id: action.id }),
        write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND business_cancelled=0 AND business_terminal=0", [run.id]));
      writes.push(write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL",
        [blocked.revision, { json: encodeActionSnapshot(blocked) }, action.id, action.revision, current.actionRow.snapshot_json as string]),
        write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["blocked", work.id as string, "action", action.id, "running"]),
        write("UPDATE business_leases SET released=1 WHERE kind=? AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at>=? AND released=0 AND attempt_id IS ? AND attempt_number IS ?",
          [input.grant.kind, input.grant.subjectID, input.grant.nodeID, input.grant.workerPrincipalID, input.grant.incarnation, input.grant.epoch,
            input.grant.expiresAt, input.grant.attemptID, input.grant.attemptNumber]));
      events.push({ type: "action.blocked", subjectID: action.id, data: json({ actionID: action.id, blocked: blocked.blocked }) });
    }
    const batch = { operationID, kind: "action.report_received", subjectID: input.actionID, request: json(request),
      evidence: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase }), result: json({ stored: false, diagnostic: true }),
      writes, ...(controlValue ? { valuePublications: [controlValue] } : { absent: [{ table: "value_reference" as const, id: ref.id }] }), ...(events.length ? { events } : {}), ...(current ? { validBefore: input.grant.expiresAt } : {}) };
    try { this.store.commit(batch); }
    catch (error) {
      if (!current || !this.definitelyFenced(error)) throw error;
      const committed = this.replay(operationID, "action.report_received", input.actionID, request);
      if (!committed.found) {
        reauthorize(); this.assertSource(input.grant, input.actionID, input.attemptID);
        const { validBefore: _deadline, events: _events, ...observational } = batch;
        this.store.commit({ ...observational, writes: common });
      }
    }
    if (transfer) {
      const receipt = bindings.diagnostic(transfer.id, transfer.workerDigest);
      if (!receipt) throw harnessError("INTERNAL_ERROR", "Diagnostic transfer receipt is unavailable");
      await bindings.finishDiagnosticRetirement(receipt);
    } else {
      if (!verifyDirectDiagnostic(this.store, batch)) throw harnessError("INTERNAL_ERROR", "Direct diagnostic receipt is unavailable");
      await values.discard(ref.id, ref.owner);
      if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report staging authority changed");
      reauthorize(); this.assertSource(input.grant, input.actionID, input.attemptID);
    }
    return { stored: false, diagnostic: true };
  }

  private publishStagedReport(input: StagedReportSource, ref: ValuePublication, reauthorize: () => boolean,
    transfer: ReportTransfer | undefined, epoch: number, limit: number | null): { stored: true } {
    if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report staging authority changed");
    const values = new ValueStore(this.store, Number.MAX_SAFE_INTEGER);
    const businessAuthorized = reauthorize();
    this.assertSource(input.grant, input.actionID, input.attemptID);
    const operationID = identity("report", [input.actionID, input.attemptID, input.phase]);
    const previous = this.store.query("SELECT request_json FROM history WHERE operation_id=?", [operationID])[0];
    const reportID = (previous ? decode<{ reportID?: string }>(previous.request_json).reportID : undefined) ?? createHash("sha256").update(operationID).digest("hex");
    const request = { actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, assignmentID: input.assignmentID, reportID,
      source: { nodeID: input.grant.nodeID, incarnation: input.grant.incarnation, workerPrincipalID: input.grant.workerPrincipalID, epoch: input.grant.epoch }, reportValue: ref,
      ...(transfer ? { transfer } : {}) };
    let prior: { found: boolean; result: Json };
    try { prior = this.replay(operationID, "action.report_received", input.actionID, request); }
    catch (error) {
      if (!(error instanceof HarnessError) || error.info.code !== "REQUEST_CONFLICT") throw error;
      // Transfer changes do not prove a different external result. The
      // transport must still recover its original transfer to replay.
      const previousRequest = previous ? decode<{ actionID?: string; attemptID?: string; phase?: string; assignmentID?: string; source?: Json; reportValue?: ValuePublication }>(previous.request_json) : null;
      const oldRef = previousRequest?.reportValue;
      if (previousRequest && oldRef && previousRequest.actionID === input.actionID && previousRequest.attemptID === input.attemptID
          && previousRequest.phase === input.phase && previousRequest.assignmentID === input.assignmentID && canonicalJson(previousRequest.source) === canonicalJson(request.source)
          && oldRef.owner === ref.owner && oldRef.encoding === ref.encoding && oldRef.length === ref.length && oldRef.digest === ref.digest) throw error;
      const conflictDigest = createHash("sha256").update(canonicalJson(request)).digest("hex");
      this.store.commit({ operationID: identity("report-conflict", [input.actionID, input.attemptID, input.phase, conflictDigest]), kind: "action.report_conflict", subjectID: input.actionID,
        request: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, digest: conflictDigest }), writes: [], evidence: json({ conflict: true }), result: null,
        events: [{ type: "action.report_conflict", subjectID: input.actionID, data: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, digest: conflictDigest }) }] });
      throw error;
    }
    if (prior.found) {
      if (transfer) {
        const binding = this.store.query("SELECT report_value_id,source_digest,worker_digest,state FROM report_transfer_bindings WHERE id=?", [transfer.id])[0];
        if (!binding || binding.report_value_id !== ref.id || binding.source_digest !== transfer.sourceDigest || binding.worker_digest !== transfer.workerDigest || binding.state !== "published")
          throw harnessError("EXECUTION_FENCED", "Report transfer publication is unavailable");
      }
      return prior.result as unknown as { stored: true };
    }
    const attempt = this.store.query("SELECT stage,execute_dispatched FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [input.attemptID, input.actionID])[0];
    if (!attempt || attempt.stage !== input.phase || input.phase === "execute" && attempt.execute_dispatched !== 1) throw harnessError("EXECUTION_FENCED", "Report does not correspond to a dispatched phase");
    const candidate = this.reportCurrent(input.actionID), receivedAt = this.now();
    const insert = (authority: LeaseGrant | null) => write("INSERT INTO action_reports(id,action_id,attempt_id,phase,assignment_id,report_json,report_value_id,authority_json,received_at) VALUES(?,?,?,?,?,?,?,?,?)", [reportID, input.actionID, input.attemptID, input.phase, input.assignmentID, { json: null }, ref.id, authority ? { json: json(authority) } : null, receivedAt]);
    const unknownLimit = limit === null;
    const quarantine = () => write("UPDATE report_processing SET state=? WHERE report_id=? AND state=?", ["deferred", reportID, "new"]);
    let current = false;
    if (businessAuthorized && candidate) try { this.leases.assert(input.grant); this.assertRunning(input.grant, candidate.action, candidate.run, candidate.work, true); current = true; } catch (error) {
      if (!this.reportEligibilityLost(error)) throw error;
    }
    const transferWrite = transfer ? new ReportTransferBindings(this.store, values).publicationWrite(transfer.id, ref.id, transfer.sourceDigest, transfer.workerDigest) : null;
    const epochWrites = epochValuePublicationWrites(this.store, ref, epoch);
    const writes = [insert(current ? input.grant : null), ...(unknownLimit ? [quarantine()] : []), ...(transferWrite ? [transferWrite] : []), ...epochWrites];
    if (current && candidate) writes.push(...this.leases.guard(input.grant), write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["report_pending", candidate.work.id as string, "action", candidate.action.id, "running"]));
    const batch = { operationID, kind: "action.report_received", subjectID: input.actionID, request: json(request), evidence: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase }), result: json({ stored: true }), writes, valuePublications: [ref],
      ...(unknownLimit ? { events: [{ type: "action.report_limit_unknown", subjectID: input.actionID, data: json({ actionID: input.actionID, attemptID: input.attemptID, reportID, reason: "legacy_attempt_limit_unavailable", phase: input.phase }) }] } : {}),
      ...(current ? { validBefore: input.grant.expiresAt } : {}) };
    try { return this.store.commit(batch).result as unknown as { stored: true }; }
    catch (error) {
      if (!current || !this.definitelyFenced(error)) throw error;
      const committed = this.replay(operationID, "action.report_received", input.actionID, request);
      if (committed.found) return committed.result as unknown as { stored: true };
      reauthorize(); this.assertSource(input.grant, input.actionID, input.attemptID);
      const { validBefore: _deadline, ...observational } = batch;
      return this.store.commit({ ...observational, writes: [insert(null), ...(unknownLimit ? [quarantine()] : []), ...(transferWrite ? [transferWrite] : []), ...epochWrites] }).result as unknown as { stored: true };
    }
  }

  /** Read only the current same-Attempt decision; never extends its grant. */
  continuation(input: { grant: LeaseGrant; actionID: string; attemptID: string }): { state: "pending" | "execute" | "stop" } {
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    this.leases.assert(input.grant);
    const candidate = this.reportCurrent(input.actionID);
    if (!candidate) return { state: "stop" };
    const { action, run, work } = candidate;
    if (action.outcome || action.cancelRequested || run.outcome || run.cancelRequested || action.latestAttempt?.id !== input.attemptID || !this.options.canDispatchRun(run.id)) return { state: "stop" };
    const attempt = this.store.query("SELECT mode,stage,execute_dispatched FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [input.attemptID, input.actionID])[0];
    if (!attempt || attempt.mode !== "automatic-recovery") return { state: "stop" };
    if (attempt.stage === "execute" && attempt.execute_dispatched === 0 && work.stage === "running") {
      const authorization = this.store.query("SELECT report_id FROM action_retry_authorizations WHERE action_id=? AND attempt_id=? AND consumed=0", [input.actionID, input.attemptID])[0];
      return { state: authorization ? "execute" : "stop" };
    }
    if (attempt.stage === "reconcile" && work.stage === "report_pending") return { state: "pending" };
    return { state: "stop" };
  }

  /** Separate diagnostic identity: disposal failures never replace a business report. */
  recordDiagnostic(input: { grant: LeaseGrant; actionID: string; attemptID: string; phase: ActionPhase; diagnosticID: string; code: string }, businessAuthorized = true): { stored: true } {
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    if (!id(input.diagnosticID) || !["execute", "reconcile"].includes(input.phase) || !["DISPOSE_FAILED", "DEPENDENCY_CREATE_FAILED", "POLICY_FAILED", "POLICY_REJECTED", "INPUT_INVALID", "VERSION_UNAVAILABLE", "RECONCILIATION_UNAVAILABLE"].includes(input.code)) throw harnessError("INVALID_ARGUMENT", "Invalid Action diagnostic");
    this.assertSource(input.grant, input.actionID, input.attemptID);
    const operationID = identity("diagnostic", [input.attemptID, input.diagnosticID]);
    const prior = this.replay(operationID, "action.diagnostic", input.actionID, input);
    if (prior.found) return prior.result as unknown as { stored: true };
    const writes: SQLWrite[] = [];
    const events: HistoryEvent[] = [{ type: "action.diagnostic", subjectID: input.actionID, data: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, code: input.code }) }];
    let validBefore: number | undefined;
    if (businessAuthorized && input.code !== "DISPOSE_FAILED") {
      const candidate = this.reportCurrent(input.actionID);
      let eligible = false;
      if (candidate) try { this.leases.assert(input.grant); this.assertRunning(input.grant, candidate.action, candidate.run, candidate.work, true); eligible = true; }
      catch (error) { if (!this.reportEligibilityLost(error)) throw error; }
      if (eligible && candidate) {
        const { action, run, actionRow, work } = candidate;
        const issue = input.code === "POLICY_REJECTED" ? { code: "FORBIDDEN", message: "Capacity policy rejected the operation", details: { phase: input.phase } }
          : input.code === "VERSION_UNAVAILABLE" ? { code: "VERSION_UNAVAILABLE", message: "Fixed Capacity or policy version is unavailable", details: { phase: input.phase } }
          : input.code === "INPUT_INVALID" ? { code: "INVALID_ARGUMENT", message: "Capacity input validation failed", details: { phase: input.phase } }
          : input.code === "POLICY_FAILED" ? { code: "STATE_CONFLICT", message: "Capacity policy could not authorize the operation", details: { phase: input.phase } }
          : input.code === "RECONCILIATION_UNAVAILABLE" ? { code: "RECONCILIATION_REQUIRED", message: "Capacity has no reconciliation function", details: { phase: input.phase } }
          : { code: "STATE_CONFLICT", message: "Capacity dependency creation failed", details: { phase: input.phase } };
        const at = this.now();
        const recheck = action.latestAttempt?.mode === "recheck-only";
        const updated: ActionView | ActionExecutionState = { ...action, blocked: issue, revision: action.revision + 1, updatedAt: at,
          ...(recheck ? { latestAttempt: { ...action.latestAttempt!, finishedAt: at } } : {}) };
        writes.push(...this.leases.guard(input.grant), write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["blocked", work.id as string, "action", action.id, "running"]),
          write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND run_id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS ?", [updated.revision, { json: encodeActionSnapshot(updated) }, action.id, run.id, action.revision, actionRow.snapshot_json as string, actionRow.outcome_json as string | null]));
        if (recheck) writes.push(write("UPDATE business_leases SET released=1 WHERE kind=? AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at>=? AND released=0 AND attempt_id IS ? AND attempt_number IS ?", [input.grant.kind, input.grant.subjectID, input.grant.nodeID, input.grant.workerPrincipalID, input.grant.incarnation, input.grant.epoch, input.grant.expiresAt, input.grant.attemptID, input.grant.attemptNumber]));
        events.push({ type: "action.blocked", subjectID: input.actionID, data: json({ actionID: input.actionID, attemptID: input.attemptID, blocked: issue }) });
        validBefore = input.grant.expiresAt;
      }
    }
    const batch = { operationID, kind: "action.diagnostic", subjectID: input.actionID, request: json(input),
      writes, evidence: json({ actionID: input.actionID, attemptID: input.attemptID, phase: input.phase, code: input.code }), result: { stored: true }, events,
      ...(validBefore === undefined ? {} : { validBefore }) };
    try { return this.store.commit(batch).result as unknown as { stored: true }; }
    catch (error) {
      if (validBefore === undefined || !this.definitelyFenced(error)) throw error;
      const committed = this.replay(operationID, "action.diagnostic", input.actionID, input);
      if (committed.found) return committed.result as unknown as { stored: true };
      this.assertSource(input.grant, input.actionID, input.attemptID);
      const { validBefore: _deadline, ...observationalBatch } = batch;
      return this.store.commit({ ...observationalBatch, writes: [], events: [events[0]!] }).result as unknown as { stored: true };
    }
  }

  private current(actionID: string): { action: ActionView | ActionExecutionState; run: RunView; actionRow: Row; runRow: Row; work: Row } {
    const actionRow = this.store.query("SELECT * FROM actions WHERE id=?", [actionID])[0];
    if (!actionRow || !actionRow.snapshot_json) throw harnessError("NOT_FOUND", "Action not found");
    const action = readActionExecutionState(this.store, actionRow);
    const runRow = this.store.query("SELECT * FROM runs WHERE id=?", [action.runID])[0];
    const work = this.store.query("SELECT * FROM work_items WHERE kind=? AND subject_id=?", ["action", actionID])[0];
    if (!runRow || !work || action.id !== actionID || action.revision !== actionRow.revision) throw harnessError("INTERNAL_ERROR", "Action record is inconsistent");
    return { action, run: readRunSnapshot(this.store, runRow.snapshot_json), actionRow, runRow, work };
  }
  /** A terminal referenced snapshot may receive late observations without
   * materializing its result or restoring any business authority. */
  private reportCurrent(actionID: string): ReturnType<ActionService["current"]> | null {
    const row = this.store.query("SELECT snapshot_projection_id FROM actions WHERE id=?", [actionID])[0];
    if (!row) throw harnessError("NOT_FOUND", "Action not found");
    if (row.snapshot_projection_id === null) return this.current(actionID);
    if (readActionControl(this.store, actionID).outcomeKind === null)
      throw harnessError("STATE_CONFLICT", "Referenced active Action report processing is unavailable");
    return null;
  }
  /** Conservative projection for the owner using the ordinary SDK request ID.
   * A report already received uses its persisted digest decision on replay. */
  reportExceedsResponse(input: { actionID: string; attemptID: string; phase: ActionPhase }, reported: Json): boolean {
    if (this.options.maxResponseBytes === undefined) return false;
    const { actionRow, runRow } = this.current(input.actionID);
    const action = readInlineActionSnapshot(this.store, actionRow);
    return reportResponseBytes(this.store.domainID, runRow.principal_id as string, action, input, reported) > this.options.maxResponseBytes;
  }
  private workGuard(work: Row, actionID: string): SQLWrite { return write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["running", work.id as string, "action", actionID, "running"]); }
  private assertRunning(grant: LeaseGrant, action: ActionView | ActionExecutionState, run: RunView, work: Row, allowRecheck = false): void {
    const recheck = allowRecheck && action.latestAttempt?.mode === "recheck-only";
    if ((!recheck && (action.outcome || action.cancelRequested || run.outcome || run.cancelRequested || work.stage !== "running" || action.latestAttempt?.id !== grant.attemptID || action.latestAttempt.number !== grant.attemptNumber || !this.options.canDispatchRun(run.id))) || work.stage !== "running" || action.latestAttempt?.id !== grant.attemptID || action.latestAttempt.number !== grant.attemptNumber) throw harnessError("EXECUTION_FENCED", "Action is no longer eligible for managed operations");
  }
  private assertSource(grant: LeaseGrant, actionID: string, attemptID: string): void {
    const attempt = this.store.query("SELECT action_id,number,epoch,node_id,worker_principal_id,incarnation FROM business_attempts WHERE id=?", [attemptID])[0];
    if (!attempt || attempt.action_id !== actionID || attempt.number !== grant.attemptNumber || attempt.epoch !== grant.epoch || attempt.node_id !== grant.nodeID || attempt.worker_principal_id !== grant.workerPrincipalID || attempt.incarnation !== grant.incarnation) throw harnessError("FORBIDDEN", "Attempt source does not match");
  }
  /** Immutable Attempt source and phase guard for a staged report fragment. */
  prepareReportTransferSource(input: { grant: LeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: ActionPhase }): SQLWrite[] {
    this.authenticateReportTransferSource(input);
    const attempt = this.store.query("SELECT stage,execute_dispatched FROM action_attempt_state WHERE action_id=? AND attempt_id=?", [input.actionID, input.attemptID])[0];
    if (!attempt || attempt.stage !== input.phase || input.phase === "execute" && attempt.execute_dispatched !== 1) throw harnessError("EXECUTION_FENCED", "Report phase is unavailable");
    // business_attempts identity is immutable and retained; recheck the
    // mutable stage and dispatch under the same write lock as the fragment.
    return [write("UPDATE action_attempt_state SET stage=? WHERE action_id=? AND attempt_id=? AND stage=? AND execute_dispatched=?",
      [input.phase, input.actionID, input.attemptID, input.phase, attempt.execute_dispatched as number])];
  }
  authenticateReportTransferSource(input: { grant: LeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: ActionPhase }): void {
    this.checkIdentity(input.grant, input.actionID, input.attemptID);
    if (input.assignmentID !== input.attemptID || !["execute", "reconcile"].includes(input.phase)) throw harnessError("INVALID_ARGUMENT", "Invalid report source association");
    this.assertSource(input.grant, input.actionID, input.attemptID);
  }
  preparePublishedReportTransferSource(input: { grant: LeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: ActionPhase }): SQLWrite[] {
    this.authenticateReportTransferSource(input);
    const state = this.store.query("SELECT stage,execute_dispatched FROM action_attempt_state WHERE action_id=? AND attempt_id=?", [input.actionID, input.attemptID])[0];
    if (!state) throw harnessError("EXECUTION_FENCED", "Report Attempt is unavailable");
    return [write("UPDATE action_attempt_state SET stage=? WHERE action_id=? AND attempt_id=? AND stage=? AND execute_dispatched=?",
      [state.stage as string, input.actionID, input.attemptID, state.stage as string, state.execute_dispatched as number])];
  }
  private checkIdentity(grant: LeaseGrant, actionID: string, attemptID: string): void {
    if (!id(actionID) || !id(attemptID) || !grant || grant.kind !== "action" || grant.subjectID !== actionID || grant.attemptID !== attemptID) throw harnessError("INVALID_ARGUMENT", "Invalid Action identity");
  }
  private now(): number { const at = this.options.clock.now(); if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid clock"); return at; }
  private definitelyFenced(error: unknown): boolean {
    return error instanceof HarnessError && error.info.disposition === "rejected" && (error.info.code === "EXECUTION_FENCED" || error.info.code === "REVISION_CONFLICT");
  }
  /** Only after immutable source authentication: losing business or recheck
   * authority prevents settlement, but cannot discard that source's fact. */
  private reportEligibilityLost(error: unknown): boolean {
    return error instanceof HarnessError && error.info.disposition === "rejected"
      && (error.info.code === "EXECUTION_FENCED" || error.info.code === "FORBIDDEN");
  }
  private replay(operationID: string, kind: string, subjectID: string, request: unknown): { found: boolean; result: Json } {
    const row = this.store.query("SELECT type,subject_id,request_json,result_json FROM history WHERE operation_id=?", [operationID])[0];
    if (!row) return { found: false, result: null };
    if (row.type !== kind || row.subject_id !== subjectID || row.request_json !== canonicalJson(request)) throw harnessError("REQUEST_CONFLICT", "Action operation identity was reused");
    return { found: true, result: parseJson(row.result_json as string) };
  }
}
