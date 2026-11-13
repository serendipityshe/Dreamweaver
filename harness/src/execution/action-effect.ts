import { createHash } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, jsonEqual, parseJson, validateReport, type ActionObservation, type ActionView } from "../contract/index.js";
import { actionReportValueReference, readActionReport, readProjectedJson, type SQLiteStore, type SQLWrite } from "../persistence/index.js";

/** A cancelled Action's single dispatched effect is known only from a valid,
 * unconflicted report. Callers also CAS the Action snapshot so the observation
 * and latest Attempt cannot change before this locked evidence guard runs. */
export function resolvedEffectGuard(store: SQLiteStore, action: ActionView, observation: ActionObservation | null = action.latestObservation): SQLWrite | null {
  if (!observation || observation.source !== "execute" || !observation.validated || observation.validationError !== null
      || observation.result === null || !observation.attemptID || observation.attemptID !== action.latestAttempt?.id) return null;
  const attempts = store.query("SELECT attempt_id,execute_dispatched FROM action_attempt_state WHERE action_id=? LIMIT 2", [action.id]);
  if (attempts.length !== 1 || attempts[0]!.attempt_id !== observation.attemptID || attempts[0]!.execute_dispatched !== 1) return null;
  if (store.query("SELECT id FROM history WHERE subject_id=? AND type=? LIMIT 1", [action.id, "action.report_conflict"]).length) return null;
  return { sql: "CHECK_ACTION_EFFECT_RESOLVED", params: [action.id, observation.attemptID], expectChanges: 1 };
}

/** An explicit, validated conclusion may close a cancelled Action's particular
 * unknown-effect marker only after every dispatched execute callback has ended.
 * The immutable final report is that boundary; an expired/released lease, a
 * checkpoint diagnostic or an Attempt's finishedAt alone is not proof. */
export async function reconciledEffectGuard(store: SQLiteStore, action: ActionView, observation: ActionObservation): Promise<SQLWrite | null> {
  if (action.outcome?.kind !== "cancelled" || !action.blocked
      || !observation.validated || observation.validationError !== null || !observation.result
      || !["operator", "reconcile"].includes(observation.source)) return null;
  const details = action.blocked.details;
  const marker = details && typeof details === "object" && !Array.isArray(details) ? details : null;
  const attempts = store.query("SELECT attempt_id FROM action_attempt_state WHERE action_id=? AND execute_dispatched=1 ORDER BY attempt_id", [action.id]);
  if (!attempts.length || attempts.length !== action.executionCount || store.query("SELECT id FROM history WHERE subject_id=? AND type='action.report_conflict' LIMIT 1", [action.id]).length) return null;
  const reports = store.query("SELECT * FROM action_reports WHERE action_id=? AND phase='execute'", [action.id]);
  if (reports.length !== attempts.length) return null;
  const proof: (string | number | null)[] = [action.id, null, null, null];
  let matchesUnknown = false;
  for (const attempt of attempts) {
    const matching = reports.filter(report => report.attempt_id === attempt.attempt_id);
    if (matching.length !== 1) return null;
    const report = matching[0]!;
    if (!report) return null;
    const ref = actionReportValueReference(store, report);
    const final = ref ? await readActionReport(store, report) : parseJson(report.report_json as string);
    if (!final || typeof final !== "object" || Array.isArray(final)
        || final.kind !== "report" && !(final.kind === "diagnostic" && ["CAPACITY_THROWN", "RESULT_CONTRACT_VIOLATION"].includes(final.code as string))) return null;
    matchesUnknown ||= action.blocked.code === "RECONCILIATION_REQUIRED" && (marker?.reportID === report.id || marker?.source === "cancellation" && marker.actionID === action.id && marker.attemptID === attempt.attempt_id);
    proof.push(attempt.attempt_id as string, report.id as string, ref ? `ref:${canonicalJson(ref)}` : report.report_json as string);
  }
  // A previous authorized only-recheck can replace the original marker with
  // retryable/reportID or its own unresolved reason. It still identifies this
  // Action's unknown effect, unlike arbitrary unrelated diagnostic text.
  const previous = action.latestObservation;
  if (!matchesUnknown && previous?.source === "reconcile" && previous.validated && !previous.validationError) {
    try {
      const report = validateReport(previous.reported, "reconcile");
      const sameReason = report.payload.kind === "retryable" && marker?.reportID === previous.id
        || report.payload.kind === "unresolved" && jsonEqual(report.payload.reason, action.blocked);
      const stored = sameReason ? store.query("SELECT r.*,o.snapshot_json observation_json,o.snapshot_projection_id observation_projection_id FROM action_reports r JOIN action_observations o ON o.id=r.id AND o.action_id=r.action_id JOIN report_processing p ON p.report_id=r.id WHERE r.id=? AND r.action_id=? AND r.phase='reconcile' AND r.authority_json IS NOT NULL AND p.state='done'", [previous.id, action.id])[0] : null;
      const ref = stored ? actionReportValueReference(store, stored) : null;
      const envelope = stored ? ref ? await readActionReport(store, stored) : parseJson(stored.report_json as string) : null;
      const digest = (value: unknown) => { const hash = createHash("sha256"); for (const chunk of canonicalJsonChunks(value)) hash.update(chunk); return hash.digest("hex"); };
      const storedObservation = stored ? await readProjectedJson(store, stored.observation_json, stored.observation_projection_id, action.id) : null;
      if (stored && digest(storedObservation) === digest(previous) && digest(envelope) === digest({ kind: "report", value: previous.reported })) {
        proof[1] = previous.id; proof[2] = ref ? `ref:${canonicalJson(ref)}` : stored.report_json as string;
        proof[3] = stored.observation_projection_id === null ? stored.observation_json as string : `projection:${stored.observation_projection_id}`;
        matchesUnknown = true;
      }
    } catch { /* Invalid/untrusted prior reports cannot identify an effect. */ }
  }
  if (!matchesUnknown) return null;
  return { sql: "CHECK_ACTION_EFFECT_RECONCILED", params: proof, expectChanges: 1 };
}
