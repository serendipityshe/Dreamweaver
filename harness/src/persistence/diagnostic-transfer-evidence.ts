import { createHash } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, InternalCanonicalJsonDecoder, type Json } from "../contract/index.js";
import { ValueStore } from "./value-store.js";
import type { CommitBatch, SQLiteStore, ValuePublication } from "./sqlite.js";
import { reportValueOwner } from "./report-value.js";
import { verifyDirectDiagnostic } from "./direct-diagnostic.js";

const object = (value: unknown): value is Record<string, Json> => !!value && typeof value === "object" && !Array.isArray(value);
const digest = (value: unknown): string => { const hash = createHash("sha256"); for (const part of canonicalJsonChunks(value)) hash.update(part); return hash.digest("hex"); };

/** Called only on the newly fenced recovery connection before unfreezing an
 * uncertain diagnostic commit. Other operations retain their existing proof. */
export async function verifyDiagnosticTransferForEvidence(store: SQLiteStore, pending: CommitBatch): Promise<boolean> {
  if (pending.kind !== "action.report_received" || !object(pending.result) || pending.result.diagnostic !== true) return true;
  store.assertEvidenceReadAuthority(); const epoch = store.coordinatorEpoch;
  const req = pending.request;
  if (object(req) && !object(req.transfer)) return verifyDirectDiagnostic(store, pending) && store.coordinatorEpoch === epoch;
  if (!object(req) || !object(req.transfer) || !object(req.source) || !object(req.controlValue) || !object(req.report)
      || req.reportDigestScope !== "canonical-report-envelope-utf8-v1" || typeof req.transfer.id !== "string"
      || typeof req.transfer.workerDigest !== "string" || typeof req.transfer.sourceDigest !== "string" || typeof req.reportID !== "string"
      || typeof req.actionID !== "string" || typeof req.attemptID !== "string" || typeof req.phase !== "string"
      || typeof req.reportDigest !== "string" || typeof req.reportBytes !== "number" || !Number.isSafeInteger(req.reportBytes)
      || !store.matchesCommitEvidence(pending)) return false;
  const ref = req.controlValue as unknown as ValuePublication;
  if (!pending.valuePublications || pending.valuePublications.length !== 1 || canonicalJson(pending.valuePublications[0]) !== canonicalJson(ref)
      || ref.digest !== req.transfer.sourceDigest || ref.encoding !== "utf8-v1") return false;
  const row = store.query("SELECT * FROM report_transfer_diagnostics WHERE id=?", [req.transfer.id])[0];
  const report = store.query("SELECT * FROM action_reports WHERE id=?", [req.reportID])[0];
  const control = store.query(`SELECT v.* FROM value_manifests v JOIN value_references r ON r.value_id=v.id
    WHERE v.id=? AND r.operation_id=?`, [ref.id, pending.operationID])[0];
  if (!row || row.control_value_id !== ref.id || row.report_value_id !== `report-v4:${req.transfer.id}`
      || row.report_owner !== reportValueOwner(store.domainID, req.actionID, req.attemptID, req.phase)
      || row.report_encoding !== "utf8-v1" || row.digest_scope !== req.reportDigestScope || row.report_length !== req.reportBytes
      || row.report_digest !== req.reportDigest || row.source_digest !== req.transfer.sourceDigest || row.worker_digest !== req.transfer.workerDigest
      || row.action_id !== req.actionID || row.attempt_id !== req.attemptID || row.phase !== req.phase || row.report_id !== req.reportID
      || row.operation_id !== pending.operationID || !control || control.state !== "published" || control.owner !== ref.owner
      || control.encoding !== ref.encoding || control.total_bytes !== ref.length || control.digest !== ref.digest
      || !report || report.action_id !== req.actionID || report.attempt_id !== req.attemptID || report.phase !== req.phase
      || report.report_value_id !== null || report.authority_json !== null || report.report_json !== canonicalJson(req.report)
      || req.report.kind !== "diagnostic" || req.report.code !== "RESULT_CONTRACT_VIOLATION" || req.report.phase !== req.phase
      || req.report.digest !== req.reportDigest) return false;
  const sizes = req.report;
  if (typeof sizes.limit !== "number" || !Number.isSafeInteger(sizes.limit) || sizes.limit <= 0
      || typeof sizes.valueBytes !== "number" || !Number.isSafeInteger(sizes.valueBytes) || sizes.valueBytes < 0
      || !(sizes.payloadBytes === null || typeof sizes.payloadBytes === "number" && Number.isSafeInteger(sizes.payloadBytes) && sizes.payloadBytes >= 0)
      || !(sizes.evidenceBytes === null || typeof sizes.evidenceBytes === "number" && Number.isSafeInteger(sizes.evidenceBytes) && sizes.evidenceBytes >= 0)) return false;
  const limit = sizes.limit;
  const exceeded = (["value", "payload", "evidence"] as const).filter(key => { const value = sizes[`${key}Bytes`]; return typeof value === "number" && value > limit; });
  const originalLimit = store.query("SELECT report_value_limit FROM action_attempt_state WHERE action_id=? AND attempt_id=?", [req.actionID, req.attemptID])[0];
  if (!exceeded.length || canonicalJson(exceeded) !== canonicalJson(sizes.exceeded) || originalLimit?.report_value_limit !== sizes.limit) return false;
  const parser = new InternalCanonicalJsonDecoder(), hash = createHash("sha256"); let total = 0;
  for await (const bytes of new ValueStore(store, Number.MAX_SAFE_INTEGER).readForEvidence(ref.id, ref.owner)) { total += bytes.length; hash.update(bytes); parser.write(bytes); }
  if (total !== ref.length || hash.digest("hex") !== ref.digest) return false;
  const source = parser.finish();
  if (!object(source) || !object(source.request) || !object(source.request.params) || !object(source.request.binding)
      || !object(source.request.params.grant) || typeof source.transferID !== "string" || source.length !== req.reportBytes || source.digest !== req.reportDigest) return false;
  const p = source.request.params, grant = p.grant as Record<string, Json>;
  if (source.request.method !== "reportAction" || source.request.binding.domainID !== store.domainID
      || source.request.binding.principalID !== req.source.workerPrincipalID || p.nodeID !== req.source.nodeID || p.incarnation !== req.source.incarnation
      || p.actionID !== req.actionID || p.attemptID !== req.attemptID || p.assignmentID !== req.assignmentID || p.phase !== req.phase
      || grant.subjectID !== req.actionID || grant.attemptID !== req.attemptID || grant.nodeID !== req.source.nodeID
      || grant.incarnation !== req.source.incarnation || grant.workerPrincipalID !== req.source.workerPrincipalID || grant.epoch !== req.source.epoch) return false;
  const nodeHash = digest(p.nodeID), workerDigest = digest([store.domainID, req.source.workerPrincipalID, nodeHash]);
  if (workerDigest !== req.transfer.workerDigest || digest(["report-transfer-v4", workerDigest, source.transferID]) !== req.transfer.id) return false;
  const attempt = store.query("SELECT action_id,number,epoch,node_id,worker_principal_id,incarnation FROM business_attempts WHERE id=?", [req.attemptID])[0];
  if (!attempt || attempt.action_id !== req.actionID || attempt.number !== grant.attemptNumber || attempt.epoch !== grant.epoch
      || attempt.node_id !== grant.nodeID || attempt.worker_principal_id !== grant.workerPrincipalID || attempt.incarnation !== grant.incarnation) return false;
  const blockedEvent = pending.events?.find(event => event.type === "action.blocked" && event.subjectID === req.actionID);
  if (blockedEvent) {
    const action = store.query("SELECT json_extract(snapshot_json,'$.blocked.code') code,json_extract(snapshot_json,'$.blocked.details.reportID') report_id,json_extract(snapshot_json,'$.latestAttempt.id') attempt_id,business_terminal FROM actions WHERE id=?", [req.actionID])[0];
    const work = store.query("SELECT stage FROM work_items WHERE kind='action' AND subject_id=?", [req.actionID])[0];
    const lease = store.query("SELECT attempt_id,released FROM business_leases WHERE kind='action' AND subject_id=?", [req.actionID])[0];
    const event = store.query("SELECT data_json FROM history WHERE type='action.blocked' AND subject_id=? AND request_json=? ORDER BY sequence DESC LIMIT 1", [req.actionID, canonicalJson(req)])[0];
    if (!action || action.code !== "RECONCILIATION_REQUIRED" || action.report_id !== req.reportID || action.attempt_id !== req.attemptID
        || action.business_terminal !== 0 || work?.stage !== "blocked" || lease?.attempt_id !== req.attemptID || lease.released !== 1
        || event?.data_json !== canonicalJson(blockedEvent.data)) return false;
  }
  store.assertEvidenceReadAuthority();
  return store.coordinatorEpoch === epoch;
}
