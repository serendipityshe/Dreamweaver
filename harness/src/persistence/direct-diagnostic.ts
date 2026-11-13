import { canonicalJson, parseJson, type Json } from "../contract/index.js";
import type { CommitBatch, SQLiteStore } from "./sqlite.js";
import { ValueStore } from "./value-store.js";

const object = (value: unknown): value is Record<string, Json> => !!value && typeof value === "object" && !Array.isArray(value);

/** Independent receipt and finite diagnostic remain valid after raw bytes are retired. */
export function verifyDirectDiagnostic(store: SQLiteStore, pending: CommitBatch): boolean {
  const request = pending.request;
  if (!object(request) || !object(request.report) || !object(request.source) || typeof request.actionID !== "string"
      || typeof request.attemptID !== "string" || typeof request.phase !== "string" || typeof request.reportID !== "string"
      || typeof request.reportDigest !== "string" || typeof request.reportBytes !== "number" || typeof request.reportOwner !== "string"
      || request.reportEncoding !== "utf8-v1" || request.reportDigestScope !== "canonical-report-envelope-utf8-v1"
      || !store.matchesCommitEvidence(pending)) return false;
  const row = store.query("SELECT * FROM report_direct_diagnostics WHERE operation_id=?", [pending.operationID])[0];
  const report = store.query("SELECT * FROM action_reports WHERE id=?", [request.reportID])[0];
  const attempt = store.query("SELECT * FROM business_attempts WHERE id=?", [request.attemptID])[0];
  const state = store.query("SELECT report_value_limit FROM action_attempt_state WHERE action_id=? AND attempt_id=?", [request.actionID, request.attemptID])[0];
  if (!row || row.report_id !== request.reportID || row.report_owner !== request.reportOwner || row.report_encoding !== request.reportEncoding
      || row.report_length !== request.reportBytes || row.report_digest !== request.reportDigest || row.action_id !== request.actionID
      || row.attempt_id !== request.attemptID || row.phase !== request.phase || !report || report.action_id !== request.actionID
      || report.attempt_id !== request.attemptID || report.phase !== request.phase || report.report_value_id !== null || report.authority_json !== null
      || report.report_json !== canonicalJson(request.report) || request.report.kind !== "diagnostic" || request.report.code !== "RESULT_CONTRACT_VIOLATION"
      || request.report.phase !== request.phase || request.report.digest !== request.reportDigest || !attempt || attempt.action_id !== request.actionID
      || attempt.node_id !== request.source.nodeID || attempt.incarnation !== request.source.incarnation
      || attempt.worker_principal_id !== request.source.workerPrincipalID || attempt.epoch !== request.source.epoch
      || state?.report_value_limit !== request.report.limit) return false;
  const limit = request.report.limit, value = request.report.valueBytes, payload = request.report.payloadBytes, evidence = request.report.evidenceBytes;
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit <= 0 || typeof value !== "number" || !Number.isSafeInteger(value) || value < 0
      || !(payload === null || typeof payload === "number" && Number.isSafeInteger(payload) && payload >= 0)
      || !(evidence === null || typeof evidence === "number" && Number.isSafeInteger(evidence) && evidence >= 0)) return false;
  const exceeded = ([ ["value", value], ["payload", payload], ["evidence", evidence] ] as const).filter(([, bytes]) => bytes !== null && bytes > limit).map(([name]) => name);
  if (!exceeded.length || canonicalJson(exceeded) !== canonicalJson(request.report.exceeded)) return false;
  const blocked = pending.events?.find(event => event.type === "action.blocked" && event.subjectID === request.actionID);
  if (blocked) {
    const action = store.query("SELECT json_extract(snapshot_json,'$.blocked.details.reportID') report_id,json_extract(snapshot_json,'$.latestAttempt.id') attempt_id,business_terminal FROM actions WHERE id=?", [request.actionID])[0];
    const work = store.query("SELECT stage FROM work_items WHERE kind='action' AND subject_id=?", [request.actionID])[0];
    const lease = store.query("SELECT attempt_id,released FROM business_leases WHERE kind='action' AND subject_id=?", [request.actionID])[0];
    const event = store.query("SELECT data_json FROM history WHERE type='action.blocked' AND subject_id=? AND request_json=? ORDER BY sequence DESC LIMIT 1", [request.actionID, canonicalJson(request)])[0];
    if (!action || action.report_id !== request.reportID || action.attempt_id !== request.attemptID || action.business_terminal !== 0
        || work?.stage !== "blocked" || lease?.attempt_id !== request.attemptID || lease.released !== 1 || event?.data_json !== canonicalJson(blocked.data)) return false;
  }
  return true;
}

export async function resumeDirectDiagnosticRetirements(store: SQLiteStore, values: ValueStore): Promise<void> {
  store.assertAuthority(); let cursor = "";
  for (;;) {
    const rows = store.query("SELECT d.*,h.request_json,h.data_json,h.result_json FROM report_direct_diagnostics d JOIN history h ON h.operation_id=d.operation_id WHERE d.operation_id>? AND EXISTS (SELECT 1 FROM value_manifests v WHERE v.id=d.report_value_id) ORDER BY d.operation_id LIMIT 64", [cursor]);
    if (!rows.length) return;
    for (const row of rows) {
      const request = parseJson(row.request_json as string), evidence = parseJson(row.data_json as string), result = parseJson(row.result_json as string);
      const pending: CommitBatch = { operationID: row.operation_id as string, kind: "action.report_received", subjectID: row.action_id as string, request, evidence, result, writes: [] };
      if (!verifyDirectDiagnostic(store, pending)) throw new Error("Direct diagnostic recovery evidence is inconsistent");
      const value = values.status(row.report_value_id as string, row.report_owner as string);
      if (value.state !== "deleting" || value.encoding !== row.report_encoding || value.length !== row.report_length || value.digest !== row.report_digest)
        throw new Error("Direct diagnostic retirement is inconsistent");
      await values.discard(value.id, value.owner); cursor = row.operation_id as string;
    }
  }
}
