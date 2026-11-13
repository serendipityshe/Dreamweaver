import { runInputBinding } from "./run-input.js";
import { harnessError, parseJson, validateRPCResponse, type Json, type RunView } from "../contract/index.js";
import type { SQLiteStore } from "./sqlite.js";
import { continuationBinding } from "./continuation.js";

/** Authorization metadata is not a native business result. Referenced child
 * input/config remain null here; only Run snapshot readers restore payloads. */
export type RunControl = Omit<RunView, "outcome"> & { outcome: { kind: NonNullable<RunView["outcome"]>["kind"] } | null };

export function readRunControl(store: SQLiteStore, encoded: unknown): RunControl {
  try {
    const stored = parseJson(encoded as string) as Record<string, Json>;
    const { inputSource: _source, continuationSource: _continuation, ...metadata } = stored;
    runInputBinding(store, stored);
    continuationBinding(store, stored);
    const value = metadata;
    let shape: unknown = value;
    if (value?.snapshotFormat !== undefined) {
      const { snapshotFormat, outcomeSource, ...body } = value;
      const source = outcomeSource as Record<string, Json>, tag = body.outcome as Record<string, Json>;
      if (snapshotFormat !== "run-outcome-ref-v1" || body.lifecycle !== "completed" || !source
          || typeof source.frameID !== "string" || !source.frameID || typeof source.operationID !== "string" || !source.operationID
          || typeof source.digest !== "string" || !/^[a-f0-9]{64}$/.test(source.digest)
          || !tag || Object.keys(tag).join(",") !== "kind" || !["succeeded", "failed", "cancelled"].includes(tag.kind as string)) throw new Error();
      // The placeholders only validate the surrounding metadata shape. They
      // are discarded below and can never become a returned business result.
      const outcome = tag.kind === "succeeded" ? { kind: "succeeded", value: null }
        : tag.kind === "failed" ? { kind: "failed", error: { code: "SOURCE", message: "Referenced result", details: null } }
        : { kind: "cancelled", reason: "Referenced result" };
      shape = { ...body, outcome };
    }
    const response = validateRPCResponse({ protocolVersion: "harness/1", requestID: "run-control",
      binding: { domainID: store.domainID, principalID: "coordinator" }, ok: true, result: shape }, "client.getRun");
    if (!response.ok) throw new Error();
    const { outcome, ...body } = response.result;
    return { ...body, outcome: outcome ? { kind: outcome.kind } : null };
  } catch { throw harnessError("INTERNAL_ERROR", "Stored Run control is invalid"); }
}
