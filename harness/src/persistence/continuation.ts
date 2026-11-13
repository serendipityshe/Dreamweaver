import { canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, type Json } from "../contract/index.js";
import { readFrameDecision, readFrameDecisionAsync } from "./driver-value.js";
import type { SQLiteStore } from "./sqlite.js";

type Obj = Record<string, Json>;
export type ContinuationSource = { encoding: "driver-continuation-v1"; runID: string; frameID: string; operationID: string; digest: string };
const object = (value: unknown): value is Obj => !!value && typeof value === "object" && !Array.isArray(value);
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid continuation source"); }

/** The accepted Driver decision is already immutable. A continuation never
 * needs a second value upload or a second mutable identity. */
export function continuationSource(runID: string, frameID: string, operationID: string, continuation: unknown): ContinuationSource {
  return { encoding: "driver-continuation-v1", runID, frameID, operationID, digest: canonicalJsonDigest(continuation) };
}

export function encodeContinuation(store: SQLiteStore, value: Obj, explicit?: ContinuationSource): Obj {
  if (!object(value.continuation) || value.continuationSource !== undefined) return value;
  if (explicit) {
    let bytes = 0;
    for (const chunk of canonicalJsonChunks(value.continuation)) { bytes += chunk.length; if (bytes > 16_384) break; }
    if (bytes <= 16_384) return value;
  }
  let source = explicit;
  if (!source && typeof value.id === "string") {
    const row = store.query("SELECT snapshot_json FROM runs WHERE id=?", [value.id])[0];
    const stored = row ? parseJson(row.snapshot_json as string) as Obj : null;
    if (stored?.continuationSource !== undefined) {
      continuationBinding(store, stored, value.id);
      source = stored.continuationSource as unknown as ContinuationSource;
    }
  }
  if (!source) return value;
  if (source.runID !== value.id || canonicalJsonDigest(value.continuation) !== source.digest) invalid();
  return { ...value, continuation: { ...value.continuation, state: null }, continuationSource: copyJson(source) };
}

/** Verify ownership and the original Core handoff without materializing the
 * state. Authorization callers use only this control projection. */
export function continuationBinding(store: SQLiteStore, value: Obj, runID: unknown = value.id) {
  if (value.continuationSource === undefined) return null;
  const source = value.continuationSource;
  if (!object(source) || Object.keys(source).sort().join(",") !== "digest,encoding,frameID,operationID,runID"
      || source.encoding !== "driver-continuation-v1" || source.runID !== runID || typeof source.runID !== "string"
      || typeof source.frameID !== "string" || !source.frameID || typeof source.operationID !== "string" || !source.operationID
      || typeof source.digest !== "string" || !/^[a-f0-9]{64}$/.test(source.digest)
      || !object(value.continuation) || typeof value.continuation.entry !== "string" || value.continuation.state !== null) invalid();
  const row = store.query(`SELECT c.decision_json,h.request_json,h.data_json FROM frames f JOIN frame_candidates c ON c.frame_id=f.id
    JOIN history h ON h.operation_id=? AND h.type='core.suspend' AND h.subject_id=f.run_id
    WHERE f.id=? AND f.run_id=? AND f.state='committed'`, [source.operationID, source.frameID, source.runID])[0];
  if (!row) invalid();
  const request = parseJson(row.request_json as string) as Obj, proof = parseJson(row.data_json as string) as Obj;
  if (request.frameID !== source.frameID || !object(request.proposal) || request.proposal.operationID !== source.operationID
      || request.proposal.action !== "suspend" || canonicalJsonDigest(request.proposal) !== canonicalJsonDigest(proof)) invalid();
  return { source: source as unknown as ContinuationSource, decisionJSON: row.decision_json };
}

function restored(value: Obj, source: ContinuationSource, decision: ReturnType<typeof readFrameDecision>): Obj {
  if (decision.kind !== "dispatch" || canonicalJsonDigest(decision.continuation) !== source.digest
      || !object(value.continuation) || value.continuation.entry !== decision.continuation.entry) invalid();
  const { continuationSource: _source, ...body } = value;
  return { ...body, continuation: copyJson(decision.continuation) };
}
export function readContinuation(store: SQLiteStore, value: Obj, runID: unknown = value.id): Obj {
  const binding = continuationBinding(store, value, runID);
  return binding ? restored(value, binding.source, readFrameDecision(store, binding.source.frameID, binding.decisionJSON)) : value;
}
export async function readContinuationAsync(store: SQLiteStore, value: Obj, runID: unknown = value.id, evidence = false): Promise<Obj> {
  const binding = continuationBinding(store, value, runID);
  return binding ? restored(value, binding.source, await readFrameDecisionAsync(store, binding.source.frameID, binding.decisionJSON, evidence)) : value;
}
