import { canonicalJsonChunks, canonicalJsonDigest, copyJson, parseJson, type Json } from "../contract/index.js";
type Obj = { [key: string]: Json };
const object = (v: unknown): v is Obj => !!v && typeof v === "object" && !Array.isArray(v);
/** The original job retains the immutable payload. Incoming reports still pass
 * exact assignment matching before first acceptance and on replay. */
export function validationReportRequest(input: unknown, prior?: unknown): Json {
  const value = copyJson(input);
  if (!object(value) || !object(value.assignment) || !object(value.assignment.payload)) throw new Error("Invalid validation report identity");
  if (prior !== undefined) {
    const previous = parseJson(prior as string);
    if (!object(previous) || previous.encoding !== "validation-report-ref-v1") return value;
  }
  let bytes = 0;
  for (const part of canonicalJsonChunks(value.assignment.payload)) { bytes += part.length; if (bytes > 16_384) break; }
  if (bytes <= 16_384 && prior === undefined) return value;
  return { ...value, encoding: "validation-report-ref-v1", assignmentPayloadDigest: canonicalJsonDigest(value.assignment.payload),
    assignment: { ...value.assignment, payload: null } };
}
