import { agentOutputValidation, readFrameDecision, readRunSnapshot, readValidationConclusion } from "../persistence/index.js";
﻿import { canonicalJson, harnessError, parseJson, validateReport, type ActionView, type Json, type Outcome, type PreparedActionRequest, type RunView } from "../contract/index.js";
import { createHash } from "node:crypto";
import { canonicalJsonDigest, canonicalJsonChunks } from "../contract/index.js";
import { restoreRunCore } from "../../../core/src/run-core.js";
import { validateRPCRequest } from "../contract/index.js";
import { readCompletionRequest, readInlineActionSnapshot, type SQLiteStore, type SQLWrite } from "../persistence/index.js";

type Row = ReturnType<SQLiteStore["query"]>[number];
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
const nonempty = (value: unknown): value is string => typeof value === "string" && value.length > 0 && value.trim() === value;
const same = (a: unknown, b: unknown) => {
  const left = canonicalJsonChunks(a)[Symbol.iterator](), right = canonicalJsonChunks(b)[Symbol.iterator]();
  for (;;) { const x = left.next(), y = right.next(); if (x.done || y.done) return x.done === y.done; if (!Buffer.from(x.value).equals(y.value)) return false; }
};

/** A candidate built only from a persisted, independently verified source.
 * The caller includes every guard in the final observation transaction. */
export interface TrustedReportSource {
  source: "operator" | "remote"; sourceID: string; actionID: string; outcome: Outcome; evidence: Json;
  measurements: { name: string; value: number; unit: string | null }[]; guards: SQLWrite[];
}

export class TrustedReportSources {
  constructor(private readonly store: SQLiteStore, private readonly authorizeOperator: (principalID: string, request: Json, action: ActionView) => void) {}

  /** A source identity names an authenticated pending command, never a caller-supplied result. */
  operator(input: { principalID: string; requestKey: string }): TrustedReportSource {
    if (!nonempty(input.principalID) || !nonempty(input.requestKey)) throw harnessError("INVALID_ARGUMENT", "Invalid operator source identity");
    const command = this.store.query("SELECT * FROM commands WHERE principal_id=? AND request_key=?", [input.principalID, input.requestKey])[0];
    if (!command || command.operation !== "resolveAction" || command.status !== "pending") throw harnessError("FORBIDDEN", "Operator source has no pending resolution command");
    validateRPCRequest({ protocolVersion: "harness/1", requestID: "trusted-source", binding: { domainID: this.store.domainID, principalID: input.principalID }, method: "admin.resolveAction", params: decode(command.request_json) });
    const request = decode<{ requestKey: string; actionID: string; expectedRevision: number; resolution: { kind: string; result?: Json; measurements?: Json }; evidence: Json }>(command.request_json);
    if (request.requestKey !== input.requestKey || !nonempty(request.actionID) || !Number.isSafeInteger(request.expectedRevision) || request.resolution?.kind !== "settle") throw harnessError("INVALID_ARGUMENT", "Operator command is not a settle report");
    const row = this.store.query("SELECT * FROM actions WHERE id=?", [request.actionID])[0];
    if (!row?.snapshot_json) throw harnessError("NOT_FOUND", "Action not found");
    const action = readInlineActionSnapshot(this.store, row);
    this.authorizeOperator(input.principalID, decode<Json>(command.request_json), action);
    const sourceID = createHash("sha256").update(canonicalJson([input.principalID, input.requestKey])).digest("hex");
    const prior = this.store.query("SELECT id FROM action_observations WHERE source=? AND source_id=?", ["operator", sourceID])[0];
    if (action.request.kind !== "capacity" || !prior && action.revision !== request.expectedRevision || row.revision !== action.revision) throw harnessError("REVISION_CONFLICT", "Operator source no longer matches the Action");
    const report = validateReport({ payload: request.resolution.result, evidence: request.evidence, measurements: request.resolution.measurements }, "execute");
    return { source: "operator", sourceID, actionID: action.id, outcome: report.payload, evidence: report.evidence, measurements: report.measurements,
      guards: [write("UPDATE commands SET status=? WHERE principal_id=? AND request_key=? AND operation=? AND status=? AND request_json=? AND fingerprint=?", ["pending", input.principalID, input.requestKey, "resolveAction", "pending", command.request_json as string, command.fingerprint as string]),
        write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND request_json=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [action.id, action.revision, row.request_json as string, row.snapshot_json as string])] };
  }

  /** Verify the actual child completion and its persisted validation evidence. */
  remote(input: { actionID: string; childRunID: string }): TrustedReportSource {
    if (!nonempty(input.actionID) || !nonempty(input.childRunID)) throw harnessError("INVALID_ARGUMENT", "Invalid remote source identity");
    const row = this.store.query("SELECT * FROM actions WHERE id=?", [input.actionID])[0];
    const childRow = this.store.query("SELECT * FROM runs WHERE id=?", [input.childRunID])[0];
    const link = this.store.query("SELECT action_id,child_run_id FROM remote_run_links WHERE action_id=? AND child_run_id=?", [input.actionID, input.childRunID])[0];
    if (!row?.snapshot_json || !childRow?.core_json || !link) throw harnessError("FORBIDDEN", "Remote relationship is unavailable");
    const action = readInlineActionSnapshot(this.store, row), child = readRunSnapshot(this.store, childRow.snapshot_json);
    let restored: ReturnType<ReturnType<typeof restoreRunCore>["getSnapshot"]>;
    try { restored = restoreRunCore(decode(childRow.core_json)).getSnapshot(); }
    catch { throw harnessError("FORBIDDEN", "Remote Core snapshot is invalid"); }
    const parentRow = this.store.query("SELECT * FROM runs WHERE id=?", [action.runID])[0];
    const parent = parentRow && readRunSnapshot(this.store, parentRow.snapshot_json);
    if (action.request.kind !== "agent" || action.remoteRunID !== child.id || child.parentActionID !== action.id || child.outcome === null || child.lifecycle !== "completed"
        || !restored || restored.state !== "completed" || restored.runID !== child.id || restored.outcome !== child.outcome.kind
        || !parent || child.rootID !== parent.rootID || childRow.principal_id !== parentRow!.principal_id || canonicalJson(child.agent) !== canonicalJson(action.request.target)) throw harnessError("FORBIDDEN", "Remote relationship or fixed version does not match");
    const records = this.store.query("SELECT * FROM history WHERE subject_id=? AND type=? AND operation_id IS NOT NULL ORDER BY sequence", [child.id, "core.complete"]);
    if (records.length !== 1) throw harnessError("FORBIDDEN", "Remote completion evidence is missing or ambiguous");
    const proof = records[0]!;
    const request = readCompletionRequest<{ proposal: { operationID: string; action: string; after: Json }; frameID: string; decision: { kind: string; outcome: Outcome }; controlCancellation?: boolean; cancellation?: RunView["cancellation"]; validation: null | { roundID: string; jobID: string; payload: Json; conclusion: { valid: boolean; issue: Json } } }>(this.store, child.id, proof.request_json);
    const after = request.proposal.after as { runID?: string; state?: string; executionToken?: Json; suspendRequested?: boolean; outcome?: string };
    if (request.proposal.operationID !== proof.operation_id || request.proposal.action !== "complete" || canonicalJson(request.proposal.after) !== childRow.core_json
        || after.runID !== child.id || after.state !== "completed" || after.executionToken !== null || after.suspendRequested !== false || after.outcome !== child.outcome.kind
        || canonicalJson(request.proposal) !== proof.data_json) throw harnessError("FORBIDDEN", "Remote completion evidence does not match the child");
    const guards: SQLWrite[] = [write("UPDATE remote_run_links SET child_run_id=? WHERE action_id=? AND child_run_id=?", [child.id, action.id, child.id]), write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND request_json=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [action.id, action.revision, row.request_json as string, row.snapshot_json as string]),
      write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND core_json=?", [child.id, child.revision, childRow.snapshot_json as string, childRow.core_json as string])];
    if (request.controlCancellation) {
      const completed = this.store.query("SELECT data_json,request_json FROM history WHERE subject_id=? AND type='run.completed'", [child.id]).filter(event => event.request_json === proof.request_json);
      const work = this.store.query("SELECT * FROM work_items WHERE kind='run' AND subject_id=?", [child.id])[0];
      if (child.outcome.kind !== "cancelled" || !child.cancelRequested || !request.cancellation || canonicalJson(child.cancellation) !== canonicalJson(request.cancellation)
          || child.outcome.reason !== request.cancellation.reason || completed.length !== 1 || completed[0]!.data_json !== canonicalJson({ outcome: child.outcome, completedAt: child.completedAt })
          || !work || work.stage !== "done") throw harnessError("FORBIDDEN", "Remote cancellation evidence does not match");
      guards.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind='run' AND subject_id=? AND stage=?", ["done", work.id as string, child.id, "done"]));
      return { source: "remote", sourceID: child.id, actionID: action.id, outcome: child.outcome, evidence: { childRunID: child.id, operationID: proof.operation_id as string }, measurements: [], guards };
    }
    const frame = this.store.query("SELECT state FROM frames WHERE id=? AND run_id=?", [request.frameID, child.id])[0];
    const candidate = this.store.query("SELECT decision_json FROM frame_candidates WHERE frame_id=?", [request.frameID])[0];
    const completedDecision = candidate && readFrameDecision(this.store, request.frameID, candidate.decision_json);
    if (frame?.state !== "committed" || completedDecision?.kind !== "complete" || canonicalJsonDigest(completedDecision) !== canonicalJsonDigest(request.decision)
        || request.decision.kind !== "complete" || canonicalJsonDigest(request.decision.outcome) !== canonicalJsonDigest(child.outcome)) throw harnessError("FORBIDDEN", "Remote completion frame evidence does not match");
    guards.push(write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=?", ["committed", request.frameID, child.id, "committed"]));
    if (child.outcome.kind === "succeeded") {
      const validation = request.validation;
      if (!validation || !validation.conclusion.valid) throw harnessError("FORBIDDEN", "Remote successful output lacks validation evidence");
      const round = this.store.query("SELECT * FROM prepare_rounds WHERE id=?", [validation.roundID])[0];
      const job = this.store.query("SELECT * FROM validation_jobs WHERE id=?", [validation.jobID])[0];
      const expected = agentOutputValidation(this.store, child, request.frameID, childRow.principal_id as string, completedDecision, validation.payload);
      if (!round || !job || round.owner_kind !== "frame" || round.owner_id !== request.frameID || round.phase !== "output" || round.state !== "accepted"
          || round.principal_id !== childRow.principal_id || job.round_id !== validation.roundID || job.state !== "accepted" || job.payload_json !== canonicalJson(validation.payload) || !same(readValidationConclusion(this.store, job.report_json), validation.conclusion)
          || canonicalJsonDigest(validation.payload) !== canonicalJsonDigest(expected)) throw harnessError("FORBIDDEN", "Remote output validation evidence does not match");
      guards.push(write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND owner_kind=? AND owner_id=?", ["accepted", validation.roundID, "accepted", "frame", request.frameID]),
        write("UPDATE validation_jobs SET state=? WHERE id=? AND round_id=? AND state=? AND payload_json=? AND report_json=? AND assignment_id=? AND assignment_epoch=?", ["accepted", validation.jobID, validation.roundID, "accepted", job.payload_json as string, job.report_json as string, job.assignment_id as string, job.assignment_epoch as number]));
    } else if (request.validation !== null) throw harnessError("FORBIDDEN", "Unexpected remote output validation evidence");
    return { source: "remote", sourceID: child.id, actionID: action.id, outcome: child.outcome, evidence: { childRunID: child.id, operationID: proof.operation_id as string }, measurements: [], guards };
  }
}
