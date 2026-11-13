import { readRunInput, readRunInputAsync } from "./run-input.js";
import { encodeChildInput, sameChildValue } from "./child-input.js";
import { canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, type Decision, type Json, type RunView } from "../contract/index.js";
import { readFrameResult, readFrameResultAsync } from "./frame-result.js";
import { readFrameDecision, readFrameDecisionAsync } from "./driver-value.js";
import type { SQLiteStore } from "./sqlite.js";
import { readCommandProjection, readCommandProjectionSync } from "./command-value.js";
import { readFactHistory, readFactHistoryAsync } from "./fact-history.js";

/** Completed outcomes use immutable frame results; large child input/config
 * use the parent Action. Mutable control fields remain scheduling metadata. */
export function encodeRunSnapshot(view: RunView, frameID: string, operationID: string): Json {
  if (view.lifecycle !== "completed" || !view.outcome) throw harnessError("INTERNAL_ERROR", "Run result reference requires completion");
  let bytes = 0;
  for (const part of canonicalJsonChunks(view.outcome)) { bytes += part.length; if (bytes > 16_384) break; }
  if (bytes <= 16_384) return encodeChildInput(view);
  return encodeChildInput({ ...view, snapshotFormat: "run-outcome-ref-v1", outcome: { kind: view.outcome.kind },
    outcomeSource: { frameID, operationID, digest: canonicalJsonDigest(view.outcome) } });
}

/** Explicit native reconstruction. Private result markers never escape as a
 * RunView. The immutable frame and original Core receipt remain mandatory. */
export function readRunSnapshot(store: SQLiteStore, encoded: unknown): RunView {
  const steps = runSnapshotSteps(encoded), step = steps.next();
  const value = step.done ? step.value : steps.next(readFrameResult(store, step.value)).value;
  return readRunInput(store, value) as unknown as RunView;
}

export async function readRunSnapshotAsync(store: SQLiteStore, encoded: unknown): Promise<RunView> {
  const steps = runSnapshotSteps(encoded), step = steps.next();
  const value = step.done ? step.value : steps.next(await readFrameResultAsync(store, step.value)).value;
  return await readRunInputAsync(store, value) as unknown as RunView;
}

function* runSnapshotSteps(encoded: unknown): Generator<Record<string, unknown>, RunView, Json> {
  const value = (typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded)) as Record<string, Json>;
  if (!value || typeof value !== "object" || Array.isArray(value)) throw harnessError("INTERNAL_ERROR", "Invalid Run snapshot");
  if (value.snapshotFormat === undefined) return value as unknown as RunView;
  if (value.snapshotFormat !== "run-outcome-ref-v1" || typeof value.id !== "string" || value.lifecycle !== "completed")
    throw harnessError("INTERNAL_ERROR", "Invalid Run outcome encoding");
  const source = value.outcomeSource as Record<string, Json>;
  if (!source || typeof source.frameID !== "string" || typeof source.operationID !== "string" || typeof source.digest !== "string")
    throw harnessError("INTERNAL_ERROR", "Invalid Run outcome source");
  const outcome = yield { run_id: value.id, frame_id: source.frameID, operation_id: source.operationID, digest: source.digest };
  const { snapshotFormat: _format, outcomeSource: _source, ...view } = value;
  return { ...view, outcome } as unknown as RunView;
}

/** Completion events share the exact decision source used by the Run view.
 * The source is published in the same transaction as these immutable events. */
export function completionEvents(view: RunView, decision: Extract<Decision, { kind: "complete" }>, frameID: string, operationID: string) {
  const snapshot = encodeRunSnapshot(view, frameID, operationID) as Record<string, Json>;
  const referenced = snapshot.snapshotFormat === "run-outcome-ref-v1";
  const source = referenced ? { encoding: "run-result-history-v1", outcomeSource: snapshot.outcomeSource } : {};
  return [
    { type: "run.decision_committed", subjectID: view.id, data: copyJson({ ...source, frameID, decision: referenced ? { ...decision, outcome: null } : decision }) },
    { type: "run.completed", subjectID: view.id, data: copyJson({ ...source, frameID, outcome: referenced ? null : decision.outcome, completedAt: view.completedAt }) },
  ];
}

export function readRunHistory(store: SQLiteStore, runID: string, type: string, encoded: unknown): Json {
  const event = parseJson(encoded as string);
  if (event && typeof event === "object" && !Array.isArray(event) && event.encoding === "wait-fact-history-v1") {
    if (type !== "wait.fact") throw harnessError("INTERNAL_ERROR", "Wait fact source has the wrong event type");
    return readFactHistory(store, runID, event);
  }
  if (event && typeof event === "object" && !Array.isArray(event) && event.encoding === "command-evidence-projection-v1") return readCommandProjectionSync(store, event);
  if (type === "run.accepted" && event && typeof event === "object" && !Array.isArray(event) && event.inputSource !== undefined) {
    if (event.runID !== runID) throw harnessError("INTERNAL_ERROR", "Child acceptance identity mismatch");
    const { id: _id, ...body } = readRunInput(store, { ...event, id: runID }) as Record<string, Json>;
    return body;
  }
  const source = dispatchHistorySource(store, runID, type, encoded);
  if (source) return dispatchHistoryValue(source, readFrameDecision(store, source.frameID, source.decisionJSON));
  const steps = runHistorySteps(runID, type, encoded), step = steps.next();
  return step.done ? step.value : steps.next(readFrameResult(store, step.value)).value as Json;
}

export async function readRunHistoryAsync(store: SQLiteStore, runID: string, type: string, encoded: unknown): Promise<Json> {
  const event = parseJson(encoded as string);
  if (event && typeof event === "object" && !Array.isArray(event) && event.encoding === "wait-fact-history-v1") {
    if (type !== "wait.fact") throw harnessError("INTERNAL_ERROR", "Wait fact source has the wrong event type");
    return readFactHistoryAsync(store, runID, event);
  }
  if (event && typeof event === "object" && !Array.isArray(event) && event.encoding === "command-evidence-projection-v1") return readCommandProjection(store, event);
  if (type === "run.accepted" && event && typeof event === "object" && !Array.isArray(event) && event.inputSource !== undefined) {
    if (event.runID !== runID) throw harnessError("INTERNAL_ERROR", "Child acceptance identity mismatch");
    const { id: _id, ...body } = await readRunInputAsync(store, { ...event, id: runID }) as Record<string, Json>;
    return body;
  }
  const source = dispatchHistorySource(store, runID, type, encoded);
  if (source) return dispatchHistoryValue(source, await readFrameDecisionAsync(store, source.frameID, source.decisionJSON));
  const steps = runHistorySteps(runID, type, encoded), step = steps.next();
  return step.done ? step.value : steps.next(await readFrameResultAsync(store, step.value)).value as Json;
}

export function dispatchDecisionEvent(runID: string, decision: Extract<Decision, { kind: "dispatch" }>, frameID: string, operationID: string) {
  let bytes = 0;
  for (const part of canonicalJsonChunks(decision)) { bytes += part.length; if (bytes > 16_384) break; }
  return { type: "run.decision_committed", subjectID: runID, data: bytes <= 16_384 ? copyJson({ frameID, decision })
    : { encoding: "run-dispatch-history-v1", frameID, operationID, decision: null, digest: canonicalJsonDigest(decision) } as Json };
}

function dispatchHistorySource(store: SQLiteStore, runID: string, type: string, encoded: unknown) {
  const value = parseJson(encoded as string);
  if (!value || typeof value !== "object" || Array.isArray(value) || value.encoding !== "run-dispatch-history-v1") return null;
  const invalid = (): never => { throw harnessError("INTERNAL_ERROR", "Invalid dispatch history source"); };
  if (type !== "run.decision_committed" || typeof value.frameID !== "string" || typeof value.operationID !== "string"
      || typeof value.digest !== "string" || value.decision !== null) invalid();
  const row = store.query(`SELECT c.decision_json,h.request_json FROM frames f JOIN frame_candidates c ON c.frame_id=f.id
    JOIN history h ON h.operation_id=? AND h.type='core.suspend' AND h.subject_id=f.run_id
    WHERE f.id=? AND f.run_id=? AND f.state='committed' AND EXISTS
     (SELECT 1 FROM history e WHERE e.subject_id=f.run_id AND e.type='run.decision_committed' AND e.request_json=h.request_json AND e.data_json=?)`,
    [value.operationID as string, value.frameID as string, runID, encoded as string])[0];
  if (!row) invalid();
  const request = parseJson(row.request_json as string) as Record<string, Json>, proposal = request.proposal as Record<string, Json>;
  if (request.frameID !== value.frameID || proposal?.operationID !== value.operationID || proposal?.action !== "suspend") invalid();
  return { frameID: value.frameID as string, digest: value.digest, decisionJSON: row.decision_json };
}

function dispatchHistoryValue(source: NonNullable<ReturnType<typeof dispatchHistorySource>>, decision: Decision): Json {
  if (decision.kind !== "dispatch" || canonicalJsonDigest(decision) !== source.digest) throw harnessError("INTERNAL_ERROR", "Invalid dispatch history decision");
  return copyJson({ frameID: source.frameID, decision });
}

function* runHistorySteps(runID: string, type: string, encoded: unknown): Generator<Record<string, unknown>, Json, Json> {
  const value = parseJson(encoded as string);
  if (!value || typeof value !== "object" || Array.isArray(value) || value.encoding !== "run-result-history-v1") return value;
  const source = value.outcomeSource as Record<string, Json>;
  if (!source || typeof source.frameID !== "string" || typeof source.operationID !== "string" || typeof source.digest !== "string"
      || value.frameID !== source.frameID) throw harnessError("INTERNAL_ERROR", "Invalid Run history source");
  const outcome = yield { run_id: runID, frame_id: source.frameID, operation_id: source.operationID, digest: source.digest };
  const { encoding: _encoding, outcomeSource: _source, ...body } = value;
  if (type === "run.completed" && body.outcome === null) return { ...body, outcome };
  if (type === "run.decision_committed" && body.decision && typeof body.decision === "object" && !Array.isArray(body.decision)
      && body.decision.kind === "complete" && body.decision.outcome === null) return { ...body, decision: { ...body.decision, outcome } };
  throw harnessError("INTERNAL_ERROR", "Invalid Run history result slot");
}

/** Preserve the original storage representation in CAS predicates. */
export function expectedRunSnapshot(store: SQLiteStore, previous: RunView): string {
  const row = store.query("SELECT snapshot_json FROM runs WHERE id=?", [previous.id])[0];
  if (!row || !sameChildValue(readRunSnapshot(store, row.snapshot_json), previous)) throw harnessError("REVISION_CONFLICT", "Run changed before conditional write");
  return row.snapshot_json as string;
}
