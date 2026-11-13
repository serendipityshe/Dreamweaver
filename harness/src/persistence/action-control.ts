import { canonicalJsonDigest, copyJson, harnessError, parseJson, type ActionView, type AttemptView, type Ref } from "../contract/index.js";
import type { SQLiteStore } from "./sqlite.js";
import type { SQLWrite } from "./writes.js";

/** Deliberately not an ActionView: no business result or observation is
 * represented by a placeholder. This record is a private control projection. */
export interface ActionControl {
  encoding: "action-control-v1";
  id: string; runID: string; revision: number; kind: "agent" | "capacity"; target: Ref;
  status: ActionView["status"]; outcomeKind: NonNullable<ActionView["outcome"]>["kind"] | null;
  latestAttempt: AttemptView | null; executionCount: number; cancelRequested: boolean; recheckRequested: boolean;
  blockedCode: string | null; outcomeObservationID: string | null; remoteRunID: string | null;
  updatedAt: number; settledAt: number | null;
}

export function makeActionControl(action: ActionView): ActionControl {
  return { encoding: "action-control-v1", id: action.id, runID: action.runID, revision: action.revision,
    kind: action.request.kind, target: { ...action.request.target }, status: action.status, outcomeKind: action.outcome?.kind ?? null,
    latestAttempt: action.latestAttempt ? { ...action.latestAttempt } : null, executionCount: action.executionCount,
    cancelRequested: action.cancelRequested, recheckRequested: action.recheckRequested, blockedCode: action.blocked?.code ?? null,
    outcomeObservationID: action.outcomeObservationID, remoteRunID: action.remoteRunID, updatedAt: action.updatedAt, settledAt: action.settledAt };
}

export function actionControlDigest(control: ActionControl): string {
  return canonicalJsonDigest(control);
}

export function actionControlWrite(operationID: string, control: ActionControl): SQLWrite {
  if (!operationID || !control.id || !Number.isSafeInteger(control.revision) || control.revision < 0) invalid();
  return { sql: "INSERT INTO action_controls(action_id,revision,operation_id,control_json) VALUES(?,?,?,?)",
    params: [control.id, control.revision, operationID, { json: copyJson(control) }], expectChanges: 1 };
}

/** An absent control row uses the old inline snapshot. A present control is
 * trusted only when its current revision, immutable receipt and routing match. */
export function readActionControl(store: SQLiteStore, actionID: string): ActionControl {
  const row = store.query(`SELECT a.id,a.run_id,a.action_key,a.revision,CASE WHEN c.control_json IS NULL THEN a.snapshot_json ELSE NULL END snapshot_json,a.business_terminal,a.business_cancelled,
      json_extract(a.request_json,'$.kind') request_kind,json_extract(a.request_json,'$.target.id') target_id,
      json_extract(a.request_json,'$.target.version') target_version,json_extract(a.outcome_json,'$.kind') outcome_kind,
      c.control_json,h.type receipt_type,h.subject_id receipt_subject,json_extract(h.request_json,'$.actionControlDigest') control_digest
    FROM actions a LEFT JOIN action_controls c ON c.action_id=a.id AND c.revision=a.revision
    LEFT JOIN history h ON h.operation_id=c.operation_id WHERE a.id=?`, [actionID])[0];
  if (!row) throw harnessError("NOT_FOUND", "Action not found");
  if (row.control_json === null) {
    const view = parseJson(row.snapshot_json as string) as unknown as ActionView;
    if (!view || view.id !== row.id || view.runID !== row.run_id || view.revision !== row.revision
        || !view.request || !view.original || view.request.key !== row.action_key || view.original.key !== row.action_key
        || view.request.kind !== row.request_kind || view.request.target?.id !== row.target_id || view.request.target?.version !== row.target_version
        || (view.outcome?.kind ?? null) !== (row.outcome_kind ?? null) || Number(view.outcome !== null) !== row.business_terminal
        || typeof view.cancelRequested !== "boolean" || Number(view.cancelRequested) !== row.business_cancelled) invalid();
    return makeActionControl(view);
  }
  const control = parseJson(row.control_json as string) as unknown as ActionControl;
  const fields = ["blockedCode", "cancelRequested", "encoding", "executionCount", "id", "kind", "latestAttempt", "outcomeKind", "outcomeObservationID", "recheckRequested", "remoteRunID", "revision", "runID", "settledAt", "status", "target", "updatedAt"];
  if (!control || typeof control !== "object" || Array.isArray(control) || Object.keys(control).sort().join(",") !== fields.join(",")
      || control.encoding !== "action-control-v1" || control.id !== row.id || control.runID !== row.run_id
      || control.revision !== row.revision || control.kind !== row.request_kind || !control.target
      || control.target.id !== row.target_id || control.target.version !== row.target_version
      || control.outcomeKind !== (row.outcome_kind ?? null) || Number(control.outcomeKind !== null) !== row.business_terminal
      || typeof control.cancelRequested !== "boolean" || Number(control.cancelRequested) !== row.business_cancelled || typeof control.recheckRequested !== "boolean"
      || !["accepted", "running", "settled"].includes(control.status) || !Number.isSafeInteger(control.executionCount) || control.executionCount < 0
      || !Number.isSafeInteger(control.updatedAt) || control.updatedAt < 0 || control.settledAt !== null && (!Number.isSafeInteger(control.settledAt) || control.settledAt < 0)
      || row.receipt_type !== "report.observed" || row.receipt_subject !== row.id || row.control_digest !== actionControlDigest(control)) invalid();
  return copyJson(control) as unknown as ActionControl;
}

function invalid(): never { throw harnessError("INTERNAL_ERROR", "Action control evidence is unavailable or inconsistent"); }
