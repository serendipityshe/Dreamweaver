import { hasChildFrameSource } from "./child-input.js";
import { canonicalJsonChunks, copyJson, harnessError, parseJson, validateNodeResponse, type DriverAssignment, type FinalizeAssignment, type Json } from "../contract/index.js";
import { readFrameInput } from "./frame-input.js";
import type { SQLiteStore } from "./sqlite.js";

type Row = Record<string, unknown>;
type FrameAssignment = DriverAssignment | FinalizeAssignment;

function invalid(): never { throw harnessError("INTERNAL_ERROR", "Frame assignment reference is unavailable or inconsistent"); }

/** Construct only after validating the assignment's input against this exact
 * immutable frame. The descriptor never stands in for the public input. */
export function makeFrameAssignmentReference(assignment: FrameAssignment, frame: Row): Json {
  if (assignment && assignment.jobID === frame.id && assignment.runID === frame.run_id && frame.input_encoding === "inline" && hasChildFrameSource(frame.input_json))
    return { encoding: "child-frame-assignment-v1", frameID: assignment.jobID, assignment: copyJson({ ...assignment, input: null }) };
  if (!assignment || assignment.jobID !== frame.id || assignment.runID !== frame.run_id || frame.input_encoding !== "wait-ref-v1"
      || typeof frame.input_operation_id !== "string" || !frame.input_operation_id) invalid();
  const metadata = copyJson({ ...assignment, input: null });
  return { encoding: "frame-assignment-v1", frameID: assignment.jobID, frameOperationID: frame.input_operation_id, assignment: metadata };
}

/** Exact comparison without constructing either complete JSON encoding. */
export async function sameFrameAssignment(left: unknown, right: unknown): Promise<boolean> {
  const a = canonicalJsonChunks(left)[Symbol.iterator](), b = canonicalJsonChunks(right)[Symbol.iterator]();
  for (;;) {
    const x = a.next(), y = b.next();
    if (x.done || y.done) return x.done === y.done;
    if (!Buffer.from(x.value).equals(Buffer.from(y.value))) return false;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
}

/** Restore an SDK assignment only from the original immutable frame and
 * its independently verified Core/wait/fact receipts. Missing discriminators
 * and stale job identity fail closed. */
export async function readFrameAssignment(store: SQLiteStore, job: Row, evidence = false): Promise<FrameAssignment> {
  const assertAuthority = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assertAuthority();
  if (typeof job.id !== "string" || typeof job.run_id !== "string" || typeof job.assignment_json !== "string") invalid();
  const stored = parseJson(job.assignment_json);
  const child = !!stored && typeof stored === "object" && !Array.isArray(stored) && stored.encoding === "child-frame-assignment-v1";
  if (job.assignment_encoding === "inline" && !child) {
    const inline = stored;
    if (inline && typeof inline === "object" && !Array.isArray(inline) && inline.encoding === "frame-assignment-v1") invalid();
    return inline as unknown as FrameAssignment;
  }
  if (job.assignment_encoding !== (child ? "inline" : "frame-ref-v1")) invalid();
  const ref = parseJson(job.assignment_json);
  if (!ref || typeof ref !== "object" || Array.isArray(ref) || Object.keys(ref).sort().join(",") !== (child ? "assignment,encoding,frameID" : "assignment,encoding,frameID,frameOperationID")
      || ref.encoding !== (child ? "child-frame-assignment-v1" : "frame-assignment-v1") || ref.frameID !== job.id || !child && typeof ref.frameOperationID !== "string"
      || !ref.assignment || typeof ref.assignment !== "object" || Array.isArray(ref.assignment) || ref.assignment.input !== null
      || ref.assignment.jobID !== job.id || ref.assignment.runID !== job.run_id || ref.assignment.assignmentID !== job.assignment_id
      || ref.assignment.assignmentEpoch !== job.assignment_epoch || ref.assignment.incarnation !== job.node_incarnation) invalid();
  const frame = store.query("SELECT id,run_id,input_json,input_encoding,input_operation_id FROM frames WHERE id=? AND run_id=?", [job.id, job.run_id])[0];
  if (!frame || (child ? frame.input_encoding !== "inline" || !hasChildFrameSource(frame.input_json) : frame.input_encoding !== "wait-ref-v1" || frame.input_operation_id !== ref.frameOperationID)) invalid();
  const input = await readFrameInput(store, frame, evidence);
  const metadata = copyJson(ref.assignment) as { [key: string]: Json }; metadata.input = copyJson(input);
  const response = validateNodeResponse({ protocolVersion: "harness/1", requestID: "frame-assignment-validation",
    binding: { domainID: store.domainID, principalID: "frame-assignment-validation" }, ok: true, result: { assignment: metadata } }, "claim");
  if (!response.ok || !response.result.assignment || !("driver" in response.result.assignment || "finalize" in response.result.assignment)) invalid();
  const table = "driver" in response.result.assignment ? "driver_jobs" : "finalize_jobs";
  const current = store.query(`SELECT assignment_json,assignment_encoding,assignment_id,assignment_epoch,node_incarnation FROM ${table} WHERE id=? AND run_id=?`, [job.id, job.run_id])[0];
  if (!current || current.assignment_json !== job.assignment_json || current.assignment_encoding !== job.assignment_encoding
      || current.assignment_id !== job.assignment_id || current.assignment_epoch !== job.assignment_epoch || current.node_incarnation !== job.node_incarnation) invalid();
  assertAuthority();
  return response.result.assignment;
}
