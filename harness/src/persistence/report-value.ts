import { createHash } from "node:crypto";
import { InternalCanonicalJsonDecoder, canonicalJson, canonicalJsonChunks, canonicalJsonDigest, harnessError, parseJson, type ActionReport, type Json } from "../contract/index.js";
import { ValueStore } from "./value-store.js";
import type { SQLiteStore, ValuePublication } from "./sqlite.js";

type ReportRow = Record<string, unknown>;

/** Stable business source used by every report intake and reread. */
export function reportValueOwner(domainID: string, actionID: string, attemptID: string, phase: string): string {
  return canonicalJson([domainID, actionID, attemptID, phase]);
}
export function reportOperationID(actionID: string, attemptID: string, phase: string): string {
  return `action.report:${canonicalJsonDigest([actionID, attemptID, phase])}`;
}

/** Legacy inline reports predate report IDs in the receipt. The immutable
 * unique (action, attempt, phase) row and receipt together bind every byte. */
export function verifyInlineReportReceipt(store: SQLiteStore, row: ReportRow): void {
  if (row.report_value_id !== null || typeof row.id !== "string" || typeof row.action_id !== "string" || typeof row.attempt_id !== "string"
      || typeof row.phase !== "string" || typeof row.assignment_id !== "string" || typeof row.report_json !== "string")
    throw harnessError("INTERNAL_ERROR", "Inline report identity is invalid");
  const receipt = store.query("SELECT type,subject_id,request_json FROM history WHERE operation_id=?", [reportOperationID(row.action_id, row.attempt_id, row.phase)])[0];
  const request = receipt && typeof receipt.request_json === "string" ? parseJson(receipt.request_json) : null;
  if (!receipt || receipt.type !== "action.report_received" || receipt.subject_id !== row.action_id || !request || typeof request !== "object" || Array.isArray(request)
      || request.actionID !== row.action_id || request.attemptID !== row.attempt_id || request.phase !== row.phase || request.assignmentID !== row.assignment_id
      || request.reportID !== undefined && request.reportID !== row.id || request.report === undefined)
    throw harnessError("INTERNAL_ERROR", "Inline report receipt identity is inconsistent");
  const left = canonicalJsonChunks(request.report)[Symbol.iterator](), right = canonicalJsonChunks(parseJson(row.report_json))[Symbol.iterator]();
  for (;;) {
    const a = left.next(), b = right.next();
    if (a.done || b.done) {
      if (a.done !== b.done) throw harnessError("INTERNAL_ERROR", "Inline report receipt bytes are inconsistent");
      break;
    }
    if (!Buffer.from(a.value).equals(Buffer.from(b.value))) throw harnessError("INTERNAL_ERROR", "Inline report receipt bytes are inconsistent");
  }
}

/** Obtain the verified reference without loading its contents. An inline row
 * is null; an explicit JSON-null slot without its reference always fails closed. */
export function actionReportValueReference(store: SQLiteStore, row: ReportRow): ValuePublication | null {
  const valueID = row.report_value_id;
  if (valueID === undefined) throw harnessError("INTERNAL_ERROR", "Report storage discriminator is missing");
  if (valueID === null) {
    if (row.report_json === "null") throw harnessError("INTERNAL_ERROR", "Report reference is missing");
    return null;
  }
  if (row.report_json !== "null" || typeof valueID !== "string" || typeof row.action_id !== "string"
      || typeof row.attempt_id !== "string" || typeof row.phase !== "string" || typeof row.id !== "string") {
    throw harnessError("INTERNAL_ERROR", "Report reference identity is invalid");
  }
  const owner = reportValueOwner(store.domainID, row.action_id, row.attempt_id, row.phase);
  const operationID = reportOperationID(row.action_id, row.attempt_id, row.phase);
  const manifest = store.query(`SELECT m.id,m.owner,m.encoding,m.total_bytes,m.digest,m.written_bytes,m.state,h.type receipt_type,h.subject_id receipt_subject,h.request_json receipt_request FROM value_manifests m
    JOIN value_references v ON v.value_id=m.id AND v.operation_id=? JOIN history h ON h.operation_id=v.operation_id WHERE m.id=?`, [operationID, valueID])[0];
  if (!manifest || manifest.owner !== owner || manifest.encoding !== "utf8-v1" || manifest.state !== "published"
      || manifest.written_bytes !== manifest.total_bytes || !Number.isSafeInteger(manifest.total_bytes)
      || typeof manifest.total_bytes !== "number" || manifest.total_bytes <= 0 || typeof manifest.digest !== "string"
      || !/^[a-f0-9]{64}$/.test(manifest.digest)) throw harnessError("INTERNAL_ERROR", "Report value is unavailable or unbound");
  const ref: ValuePublication = { id: valueID, owner, encoding: "utf8-v1", length: manifest.total_bytes, digest: manifest.digest };
  const request = parseJson(manifest.receipt_request as string) as Record<string, Json>;
  if (manifest.receipt_type !== "action.report_received" || manifest.receipt_subject !== row.action_id || !request || typeof request !== "object"
      || request.actionID !== row.action_id || request.attemptID !== row.attempt_id || request.phase !== row.phase || request.reportID !== row.id
      || canonicalJson(request.reportValue) !== canonicalJson(ref)) throw harnessError("INTERNAL_ERROR", "Report receipt does not bind its value");
  return ref;
}

/** Read old inline and new reference records through one authenticated source. */
export async function readActionReport(store: SQLiteStore, row: ReportRow): Promise<ActionReport> {
  return readReport(store, row, false);
}

/** Recovery-only source read, after the old connection has been fenced. */
export async function readActionReportForEvidence(store: SQLiteStore, row: ReportRow): Promise<ActionReport> {
  return readReport(store, row, true);
}

/** Compatibility reader for synchronous control transactions. RPC readers use
 * the yielding reader above. Never joins the encoded document into a string. */
export function readActionReportSync(store: SQLiteStore, row: ReportRow, evidence = false): ActionReport {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assert(); const epoch = store.coordinatorEpoch;
  const ref = actionReportValueReference(store, row);
  if (!ref) {
    verifyInlineReportReceipt(store, row);
    return parseJson(row.report_json as string) as unknown as ActionReport;
  }
  const decoder = new InternalCanonicalJsonDecoder(), hash = createHash("sha256");
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  let offset = 0;
  while (offset < ref.length) {
    const { bytes } = values.readChunk(ref.id, ref.owner, offset, 65_536, evidence);
    if (!bytes.byteLength) throw harnessError("INTERNAL_ERROR", "Incomplete report value");
    hash.update(bytes); decoder.write(bytes); offset += bytes.byteLength;
  }
  if (offset !== ref.length || hash.digest("hex") !== ref.digest) throw harnessError("INTERNAL_ERROR", "Report value integrity failed");
  const report = decoder.finish();
  if (!report || typeof report !== "object" || Array.isArray(report) || Object.keys(report).sort().join(",") !== "kind,value" || report.kind !== "report")
    throw harnessError("INTERNAL_ERROR", "Referenced report has an invalid envelope");
  assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report reading authority changed");
  return { kind: "report", value: report.value as Json };
}

async function readReport(store: SQLiteStore, row: ReportRow, evidence: boolean): Promise<ActionReport> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assert(); const epoch = store.coordinatorEpoch;
  const ref = actionReportValueReference(store, row);
  if (!ref) {
    const report = parseJson(row.report_json as string) as unknown as ActionReport;
    assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report reading authority changed");
    return report;
  }
  const decoder = new InternalCanonicalJsonDecoder(), hash = createHash("sha256"), values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  let length = 0;
  for await (const part of evidence ? values.readForEvidence(ref.id, ref.owner) : values.read(ref.id, ref.owner)) { hash.update(part); length += part.byteLength; decoder.write(part); }
  if (length !== ref.length || hash.digest("hex") !== ref.digest) throw harnessError("INTERNAL_ERROR", "Report value integrity failed");
  const report = decoder.finish();
  if (!report || typeof report !== "object" || Array.isArray(report) || Object.keys(report).sort().join(",") !== "kind,value" || report.kind !== "report")
    throw harnessError("INTERNAL_ERROR", "Referenced report has an invalid envelope");
  assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report reading authority changed");
  return { kind: "report", value: report.value as Json };
}
