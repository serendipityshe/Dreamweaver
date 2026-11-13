import { readRunFrameAsync } from "./run-input.js";
import { canonicalJson, copyJson, parseJson, validateAdvanceInput, type AdvanceInput, type Fact, type Json, type Wake } from "../contract/index.js";
import { canonicalValueWitness } from "./canonical-value.js";
export { canonicalValueWitness } from "./canonical-value.js";
import { readFactOutcome, readFactOutcomeForEvidence } from "./projection.js";
import { initialWaitSource, readInitialWaitWakeAsync } from "./initial-wake.js";
import type { SQLiteStore } from "./sqlite.js";

type Row = Record<string, unknown>;
type ResolvedWake = Exclude<Wake, { kind: "start" }>;

export async function waitFactWitness(fact: Fact): Promise<Json> {
  return { factID: fact.id, actionID: fact.actionID, actionKey: fact.actionKey,
    encoding: "canonical-json-utf8-v1", ...await canonicalValueWitness(fact.outcome) };
}

/** Both old inline wake records and the new immutable-fact references are
 * accepted. Referenced records require the original atomic receipt. */
export async function readWaitWake(store: SQLiteStore, wait: Row, runID: string, evidence = false): Promise<ResolvedWake> {
  if (evidence) store.assertEvidenceReadAuthority(); else store.assertAuthority();
  const value = parseJson(wait.wake_json as string);
  const initial = initialWaitSource(value);
  if (initial) {
    if (initial.waitID !== wait.id || initial.frameID !== wait.frame_id || initial.runID !== runID || wait.run_id !== runID || wait.winner !== "results") throw new Error("Initial wait source identity mismatch");
    return readInitialWaitWakeAsync(store, runID, value, evidence);
  }
  if (!value || typeof value !== "object" || Array.isArray(value) || value.encoding !== "wait-fact-witness-v1" && value.encoding !== "wait-fact-witness-v2") return value as unknown as ResolvedWake;
  const projected = value.encoding === "wait-fact-witness-v2";
  if (Object.keys(value).sort().join(",") !== (projected ? "encoding,eventProjectionIDs,facts,pendingActionKeys,waitID,winner" : "encoding,facts,pendingActionKeys,waitID,winner")
      || value.waitID !== wait.id || wait.run_id !== runID || value.winner !== wait.winner
      || value.winner !== "results" && value.winner !== "timeout" || !Array.isArray(value.facts)
      || !Array.isArray(value.pendingActionKeys) || value.pendingActionKeys.some(key => typeof key !== "string")
      || projected && (!Array.isArray(value.eventProjectionIDs) || value.eventProjectionIDs.some(id => id !== null && typeof id !== "string"))) throw new Error("Invalid wait witness");
  const operationID = `wait.resolve:${wait.id}:${value.winner}`;
  const factCount = value.facts.length;
  const ids = projected ? value.eventProjectionIDs as (string | null)[] : [];
  if (projected && (ids.length !== 1 + value.facts.length + value.pendingActionKeys.length || !ids.some(id => id !== null)))
    throw new Error("Invalid wait history projection set");
  const events = projected ? ids.map(id => ({ type: "wait.fact", subjectID: runID, data: null, ...(id === null ? {} : { dataProjectionID: id }) })) : undefined;
  if (!store.matchesCommitEvidence({ operationID, kind: "wait.resolve", subjectID: runID, request: value, writes: [], evidence: null, result: null, ...(events ? { events } : {}) }))
    throw new Error("Wait witness has no matching operation receipt");
  if (projected) {
    const rows = store.query("SELECT type,subject_id,data_projection_id FROM history WHERE operation_id IS NULL AND request_json=? ORDER BY sequence", [canonicalJson(value)]);
    if (rows.length !== ids.length || rows.some((row, index) => row.subject_id !== runID || row.data_projection_id !== ids[index]
        || row.type !== (index === 0 ? "wait.resolved" : index <= factCount ? "wait.fact" : "wait.pending")))
      throw new Error("Wait history projection receipt is incomplete");
  }
  const facts: Fact[] = [];
  for (const expected of value.facts) {
    if (!expected || typeof expected !== "object" || Array.isArray(expected) || typeof expected.factID !== "string"
        || typeof expected.actionID !== "string" || typeof expected.actionKey !== "string") throw new Error("Invalid wait fact witness");
    const row = store.query("SELECT f.id,f.action_id,f.outcome_json,f.outcome_projection_id,a.action_key FROM facts f JOIN actions a ON a.id=f.action_id WHERE f.id=? AND f.action_id=? AND f.run_id=? AND a.run_id=?", [expected.factID, expected.actionID, runID, runID])[0];
    if (!row || row.action_key !== expected.actionKey) throw new Error("Wait fact source changed");
    const fact: Fact = { id: expected.factID, actionID: expected.actionID, actionKey: expected.actionKey,
      outcome: evidence ? await readFactOutcomeForEvidence(store, row) : await readFactOutcome(store, row) };
    if (canonicalJson(await waitFactWitness(fact)) !== canonicalJson(expected)) throw new Error("Wait fact digest changed");
    facts.push(fact);
  }
  if (evidence) store.assertEvidenceReadAuthority(); else store.assertAuthority();
  return value.winner === "results" ? { kind: "results", waitID: value.waitID as string, facts }
    : { kind: "timeout", waitID: value.waitID as string, facts, pendingActionKeys: value.pendingActionKeys as string[] };
}

/** Resolve a job's private storage representation against its immutable
 * frame. A missing discriminator or inconsistent reference fails closed. */
export function frameJobInput(job: Record<string, unknown>, frame: Record<string, unknown>): string | null {
  if (typeof job.id !== "string" || job.id !== frame.id || typeof job.run_id !== "string" || job.run_id !== frame.run_id
      || typeof frame.input_json !== "string") return null;
  if (job.input_encoding === "inline") return job.input_json === frame.input_json ? frame.input_json : null;
  if (job.input_encoding === "frame-ref-v1") return job.input_json === "null" ? frame.input_json : null;
  return null;
}

/** Reconstruct the native Driver input through the original wait and its
 * immutable fact set. The original Core receipt binds the descriptor. */
export async function readFrameInput(store: SQLiteStore, frame: Row, evidence = false): Promise<AdvanceInput> {
  if (evidence) store.assertEvidenceReadAuthority(); else store.assertAuthority();
  if (typeof frame.id !== "string" || typeof frame.run_id !== "string" || typeof frame.input_json !== "string") throw new Error("Frame identity is unavailable");
  if (frame.input_encoding === "inline") {
    if (frame.input_operation_id !== null) throw new Error("Invalid inline frame discriminator");
    const input = validateAdvanceInput(await readRunFrameAsync(store, parseJson(frame.input_json), evidence));
    if (input.frameID !== frame.id || input.run.id !== frame.run_id) throw new Error("Frame input identity mismatch");
    return input;
  }
  if (frame.input_encoding !== "wait-ref-v1" || typeof frame.input_operation_id !== "string") throw new Error("Frame input discriminator is unavailable");
  const ref = parseJson(frame.input_json);
  if (!ref || typeof ref !== "object" || Array.isArray(ref) || Object.keys(ref).sort().join(",") !== "digest,encoding,input,length,operationID,waitID"
      || ref.encoding !== "frame-wait-ref-v1" || ref.operationID !== frame.input_operation_id || typeof ref.waitID !== "string"
      || typeof ref.length !== "number" || !Number.isSafeInteger(ref.length) || ref.length <= 0
      || typeof ref.digest !== "string" || !/^[a-f0-9]{64}$/.test(ref.digest)
      || !ref.input || typeof ref.input !== "object" || Array.isArray(ref.input) || ref.input.frameID !== frame.id
      || !ref.input.run || typeof ref.input.run !== "object" || Array.isArray(ref.input.run) || ref.input.run.id !== frame.run_id || ref.input.wake !== null)
    throw new Error("Invalid frame input reference");
  const receipt = store.query("SELECT type,subject_id,request_json,data_json FROM history WHERE operation_id=?", [frame.input_operation_id])[0];
  if (!receipt || receipt.type !== "core.resume" || receipt.subject_id !== frame.run_id) throw new Error("Frame operation receipt is unavailable");
  const request = parseJson(receipt.request_json as string);
  if (!request || typeof request !== "object" || Array.isArray(request) || request.frameID !== frame.id || request.waitID !== ref.waitID
      || canonicalJson(request.inputRef) !== canonicalJson(ref) || !request.proposal || typeof request.proposal !== "object" || Array.isArray(request.proposal)
      || request.proposal.operationID !== frame.input_operation_id || request.proposal.action !== "resume"
      || !store.matchesCommitEvidence({ operationID: frame.input_operation_id, kind: "core.resume", subjectID: frame.run_id,
        request, writes: [], evidence: request.proposal, result: null })) throw new Error("Frame reference is not bound to its Core receipt");
  const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=?", [ref.waitID, frame.run_id])[0];
  if (!wait || wait.resumed_frame_id !== frame.id || !wait.wake_json) throw new Error("Frame wait is unavailable");
  const wake = await readWaitWake(store, wait, frame.run_id, evidence);
  const linked = store.query("SELECT fact_id FROM frame_facts WHERE frame_id=? AND run_id=? ORDER BY fact_id", [frame.id, frame.run_id]).map(item => item.fact_id);
  const expected = wake.facts.map(fact => fact.id).sort();
  if (canonicalJson(linked) !== canonicalJson(expected)) throw new Error("Frame fact set changed");
  const template = copyJson(ref.input) as Record<string, Json>;
  template.wake = copyJson(wake);
  const input = validateAdvanceInput(await readRunFrameAsync(store, template, evidence));
  const actual = await canonicalValueWitness(input);
  if (actual.length !== ref.length || actual.digest !== ref.digest) throw new Error("Frame input content changed");
  if (evidence) store.assertEvidenceReadAuthority(); else store.assertAuthority();
  return input;
}
