import { harnessError } from "../contract/index.js";

/** A report or checkpoint combines a legal value with identities copied from
 * a bounded assignment or register. Four response and payload budgets cover
 * those copies; fixed protocol fields and scalar lease numbers fit in 4096.
 * A Driver decision or rewrite list can be larger and is not bounded by V. */
export function nodeDataLimit(maxPayloadBytes: number, maxValueBytes: number, maxResponseBytes: number): number {
  if (![maxPayloadBytes, maxValueBytes, maxResponseBytes].every(value => Number.isSafeInteger(value) && value > 0))
    throw harnessError("INVALID_ARGUMENT", "Invalid node data transport budgets");
  // This is a conservative transport estimate, not a business value limit.
  // Saturate before converting to Number: an oversized estimate must not
  // prevent recovery of an existing small report.
  const estimate = 4n * BigInt(maxPayloadBytes) + BigInt(maxValueBytes) + 4n * BigInt(maxResponseBytes) + 4096n;
  return Number(estimate > BigInt(Number.MAX_SAFE_INTEGER) ? BigInt(Number.MAX_SAFE_INTEGER) : estimate);
}

export function carriesNodeData(method: string): boolean {
  return method === "reportAction" || method === "checkpointAction" || method === "verifyValidation" || method === "readValidationValue" || method === "report";
}
