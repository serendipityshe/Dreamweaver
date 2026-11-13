import { encodeRunInput } from "../persistence/index.js";
import { readRunSnapshot, type RunControl } from "../persistence/index.js";
import { createHash, randomUUID } from "node:crypto";
import { canonicalJson, copyJson, harnessError, HarnessError, jsonEqual, parseJson,
  type ActionObservation, type ActionView, type CancelRequest, type CancellationInfo, type Json, type RunView } from "../contract/index.js";
import { authorizeOperation, type PrincipalGrant } from "../message/index.js";
import { cancellationGuard, readInlineActionSnapshot, type SQLWrite, type SQLiteStore } from "../persistence/index.js";

const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });

/** Durable command admission; control completion is performed by the Run service. */
export function createCancelService(store: SQLiteStore, options: {
  clock: { now(): number };
  maxResponseBytes?: number;
  authorizeRun(id: string, principal: PrincipalGrant): RunControl;
}) {
  function checkCancellationBudget(root: RunView, reason: string | null, principalID: string, requestKey: string): SQLWrite[] {
    if (options.maxResponseBytes === undefined) return [];
    const max = Number.MAX_SAFE_INTEGER, uuid = "00000000-0000-0000-0000-000000000000";
    const cursor = Buffer.from(canonicalJson({ high: max, position: max })).toString("base64url") + "." + "A".repeat(43);
    const tooLarge = (value: unknown, owner: string) => Buffer.byteLength(canonicalJson({ protocolVersion: "harness/1", requestID: uuid,
      binding: { domainID: store.domainID, principalID: owner }, ok: true, result: value }), "utf8") > options.maxResponseBytes!;
    const cancellation = { source: "parent" as const, reason, requestedAt: max }, outcome = { kind: "cancelled" as const, reason };
    const owner = store.query("SELECT principal_id FROM runs WHERE id=?", [root.id])[0]?.principal_id;
    if (typeof owner !== "string") throw harnessError("INTERNAL_ERROR", "Cancellation owner is unavailable");
    const terminal: RunView = { ...root, revision: max, updatedAt: max, completedAt: max, lifecycle: "completed", cancelRequested: true, cancellation, outcome };
    const command = { requestKey, operation: "cancel", status: "applied", result: terminal, error: null, createdAt: max, updatedAt: max };
    const record = { id: uuid, sequence: max, at: max, runID: root.id, actionID: null, attemptID: null, type: "run.completed", data: { outcome, completedAt: max } };
    if (tooLarge(terminal, owner) || tooLarge(command, principalID) || tooLarge({ items: [record], nextCursor: cursor }, owner))
      throw harnessError("LIMIT_EXCEEDED", "Cancellation reason cannot fit in the response budget");
    const guards: SQLWrite[] = [];
    // A direct child cancellation also delivers its outcome to its parent Action.
    // Descendant propagation is deliberately not traversed in this admission path.
    if (root.parentActionID) {
      const row = store.query("SELECT a.*,r.principal_id FROM actions a JOIN runs r ON r.id=a.run_id WHERE a.id=?", [root.parentActionID])[0];
      if (!row?.snapshot_json || typeof row.principal_id !== "string") throw harnessError("INTERNAL_ERROR", "Cancellation Action is unavailable");
      if (row.snapshot_projection_id !== null) throw harnessError("STATE_CONFLICT", "Referenced Action cancellation is not yet available");
      const action = readInlineActionSnapshot(store, row);
      guards.push(write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [action.id, action.revision, row.snapshot_json as string]));
      const next: ActionView = { ...action, revision: max, updatedAt: max, status: "settled", cancelRequested: true, cancellation,
        outcome: action.outcome ?? outcome, outcomeObservationID: action.outcomeObservationID, settledAt: max,
        blocked: action.blocked ?? (action.executionCount > 0 ? { code: "RECONCILIATION_REQUIRED", message: "External effect remains unknown after cancellation", details: { actionID: action.id, attemptID: action.latestAttempt?.id ?? null, source: "cancellation" } } : null) };
      if (tooLarge(next, row.principal_id) || tooLarge({ items: [next], nextCursor: cursor }, row.principal_id))
        throw harnessError("LIMIT_EXCEEDED", "Cancellation Action cannot fit in the response budget");
      if (action.remoteRunID) {
        const observationID = `trusted:${createHash("sha256").update(canonicalJson(["remote", action.remoteRunID])).digest("hex")}`;
        const evidence = { childRunID: action.remoteRunID, operationID: uuid };
        const observation: ActionObservation = { id: observationID, attemptID: null, source: "remote", observedAt: max, result: outcome, evidence,
          validationError: null, measurements: [], reported: json({ payload: outcome, evidence, measurements: [] }), validated: true, acceptedAsOutcome: false };
        const withObservation: ActionView = { ...next, latestObservation: observation };
        const event = { id: uuid, sequence: max, at: max, runID: action.runID, actionID: action.id, attemptID: null, type: "action.observed", data: { actionID: action.id, observation } };
        if (tooLarge(withObservation, row.principal_id) || tooLarge({ items: [withObservation], nextCursor: cursor }, row.principal_id) || tooLarge({ items: [event], nextCursor: cursor }, row.principal_id))
          throw harnessError("LIMIT_EXCEEDED", "Cancellation observation cannot fit in the response budget");
      }
    }
    return guards;
  }
  return {
    cancel(request: CancelRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant = () => principal, requestID = "00000000-0000-0000-0000-000000000000", fragmentedResponses = false): RunView {
      for (;;) {
        store.assertAuthority();
        const grant = revalidate();
        authorizeOperation(grant, "client.requestCancel");
        const existing = store.query("SELECT * FROM commands WHERE principal_id=? AND request_key=?", [principal.principalID, request.requestKey])[0];
        if (existing) {
          const original = decode<CancelRequest>(existing.request_json);
          if (existing.operation === "cancel") options.authorizeRun(original.runID, grant);
          else options.authorizeRun(request.runID, grant);
          if (existing.operation !== "cancel" || !jsonEqual(original, request)) throw harnessError("REQUEST_CONFLICT", "Request key is bound to a different command");
          if (existing.status !== "applied" || existing.result_json === null) throw harnessError("INTERNAL_ERROR", "Cancel command status is unavailable", null, "uncertain");
          return readRunSnapshot(store, existing.result_json);
        }
        const control = options.authorizeRun(request.runID, grant);
        const row = store.query("SELECT snapshot_json,revision FROM runs WHERE id=?", [request.runID])[0];
        if (!row) throw harnessError("NOT_FOUND", "Run was not found");
        const persisted = readRunSnapshot(store, row.snapshot_json);
        const view: RunView = { ...persisted, rootActionCount: control.rootActionCount, cancelRequested: control.cancelRequested, cancellation: control.cancellation };
        // The authorized projection and CAS snapshot must describe the same Run revision.
        if (control.revision !== persisted.revision || row.revision !== persisted.revision) continue;
        const at = options.clock.now();
        if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock");
        const first = !persisted.outcome && !persisted.cancelRequested && !view.cancelRequested;
        const budgetGuards = first && !fragmentedResponses ? checkCancellationBudget(persisted, request.reason, principal.principalID, request.requestKey) : [];
        const cancellation: CancellationInfo = { source: "client", reason: request.reason, requestedAt: at };
        const result: RunView = first ? { ...persisted, revision: persisted.revision + 1, updatedAt: at, cancelRequested: true, cancellation } : view;
        // A completed Run keeps its original immutable result source. The
        // command freezes response metadata without duplicating the payload.
        const stored = decode<Record<string, Json>>(row.snapshot_json);
        const resultRecord = stored.snapshotFormat === "run-outcome-ref-v1"
          ? encodeRunInput(store, { ...result, snapshotFormat: stored.snapshotFormat, outcome: stored.outcome, outcomeSource: stored.outcomeSource }) : encodeRunInput(store, result);
        // The response's real transport identity may be much larger than an
        // SDK UUID. Reject before writing the command if this result cannot fit.
        if (!fragmentedResponses && options.maxResponseBytes !== undefined && Buffer.byteLength(canonicalJson({ protocolVersion: "harness/1", requestID,
          binding: { domainID: store.domainID, principalID: principal.principalID }, ok: true, result }), "utf8") > options.maxResponseBytes) {
          throw harnessError("LIMIT_EXCEEDED", "Cancellation response exceeds the response limit");
        }
        const fingerprint = createHash("sha256").update(canonicalJson(request)).digest("hex");
        const writes: SQLWrite[] = [write("INSERT INTO commands (principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
          [principal.principalID, request.requestKey, "cancel", { json: json(request) }, fingerprint, "applied", { json: resultRecord }, null, at, at])];
        if (first) {
          const work = store.query("SELECT id,stage FROM work_items WHERE kind=? AND subject_id=?", ["run", request.runID])[0];
          if (!work || typeof work.id !== "string" || typeof work.stage !== "string") throw harnessError("INTERNAL_ERROR", "Run control work is unavailable");
          const barrierID = randomUUID();
          writes.push(...budgetGuards, cancellationGuard({ kind: "run", id: request.runID }),
            write("UPDATE runs SET revision=?,snapshot_json=? WHERE id=? AND revision=? AND snapshot_json=?", [result.revision, { json: encodeRunInput(store, result) }, request.runID, row.revision as number, row.snapshot_json as string]),
            write("INSERT INTO cancellation_barriers(id,scope,run_id,action_id,cancellation_json) VALUES(?,?,?,?,?)", [barrierID, "run", request.runID, null, { json: json(cancellation) }]),
            write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["cancel", work.id, "run", request.runID, work.stage]),
            write("INSERT INTO work_items(id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES(?,?,?,?,?,?,?,?)", [JSON.stringify(["cancel.cleanup", request.runID]), "cancel_cleanup", request.runID, "pending", 0, at, { json: json({ runID: request.runID, barrierID }) }, null]));
        } else {
          // Make the returned view and command receipt compete with concurrent completion/cancellation.
          writes.push(write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [request.runID, row.revision as number, row.snapshot_json as string]));
        }
        try {
          const receipt = store.commit({ operationID: `cancel:${canonicalJson([principal.principalID, request.requestKey])}`, kind: "cancel.request", subjectID: request.runID,
            request: json(request), writes, result: resultRecord, evidence: json({ first }),
            events: [{ type: "run.cancel_requested", subjectID: request.runID, data: json({ requestKey: request.requestKey, first, cancellation: first ? cancellation : view.cancellation }) }] });
          return readRunSnapshot(store, receipt.result);
        } catch (error) {
          if (error instanceof HarnessError && error.info.code === "REVISION_CONFLICT" && error.info.disposition !== "uncertain") continue;
          if (first && error instanceof HarnessError && error.info.code === "EXECUTION_FENCED"
              && store.effectiveCancellation({ kind: "run", id: request.runID })) continue;
          throw error;
        }
      }
    },
  };
}
