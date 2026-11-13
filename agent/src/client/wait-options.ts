import { HarnessError } from "../../../harness/src/index.js";
import type { WaitOptions } from "../contract/types.js";
import { fail, fields, positive } from "../contract/common.js";

/** Validate and snapshot options for RunHandle.wait. */
export function validateWaitOptions(options: WaitOptions = {}): WaitOptions {
  fields(options, ["timeoutMs", "signal", "poll"]); fields(options.poll ?? {}, ["initialMs", "maxMs"]);
  const timeoutMs = options.timeoutMs ?? 60000, initialMs = options.poll?.initialMs ?? 250, maxMs = options.poll?.maxMs ?? 1000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) fail("Invalid wait timeout");
  positive(initialMs); positive(maxMs); if (initialMs > maxMs) fail("Invalid polling interval");
  if (options.signal !== undefined && !(options.signal instanceof AbortSignal)) fail("Invalid wait signal");
  if (options.signal?.aborted) throw new HarnessError({ code: "CALL_ABORTED", message: "Waiting was aborted", disposition: "rejected", details: null, retryAfterMs: null });
  return { timeoutMs, poll: { initialMs, maxMs }, ...(options.signal ? { signal: options.signal } : {}) };
}
