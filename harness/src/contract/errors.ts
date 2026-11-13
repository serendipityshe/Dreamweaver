// Shared infrastructure error representation.
import type { ErrorCode, ErrorInfo, Json } from "./types.js";
import { copyJson } from "./json.js";

/** Infrastructure errors; business failures remain Outcome.failed. */
export class HarnessError extends Error {
  readonly info: ErrorInfo;

  constructor(info: ErrorInfo, options?: ErrorOptions) {
    super(info.message, options);
    this.name = "HarnessError";
    this.info = Object.freeze({ ...info, details: copyJson(info.details) });
  }
}

export function harnessError(
  code: ErrorCode,
  message: string,
  details: Json = null,
  disposition: ErrorInfo["disposition"] = code === "TRANSPORT_UNCERTAIN" ? "uncertain" : "rejected",
  retryAfterMs: number | null = null,
): HarnessError {
  return new HarnessError({ code, message, details, disposition, retryAfterMs });
}

/** The caller must know whether a write may have committed; never infer rejection. */
export function toHarnessError(error: unknown, possiblyCommitted: boolean): HarnessError {
  if (error instanceof HarnessError) {
    return possiblyCommitted && error.info.disposition !== "uncertain"
      ? new HarnessError({ ...error.info, disposition: "uncertain" }, { cause: error })
      : error;
  }
  return new HarnessError({
    code: "INTERNAL_ERROR",
    message: "Harness operation failed",
    disposition: possiblyCommitted ? "uncertain" : "rejected",
    details: null,
    retryAfterMs: null,
  }, { cause: error });
}
