import { createHash } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, copyJson, harnessError, parseJson, type Fact, type Json, type Wake } from "../contract/index.js";
import { readFactOutcome, readFactOutcomeForEvidence, readProjectedJsonSync } from "./projection.js";
import type { SQLiteStore, HistoryEvent, ValuePublication } from "./sqlite.js";
type Obj = Record<string, Json>;
type Row = Record<string, unknown>;
const object = (value: unknown): value is Obj => !!value && typeof value === "object" && !Array.isArray(value);
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Initial wait fact source is unavailable or inconsistent"); }
export interface InitialWaitSource { encoding: "initial-wait-fact-witness-v1"; runID: string; frameID: string; waitID: string; operationID: string; facts: Json[] }
export function initialWakeNeedsReference(wake: unknown): boolean {
  let size = 0; for (const bytes of canonicalJsonChunks(wake)) { size += bytes.length; if (size > 16384) return true; } return false;
}
export function makeInitialWaitWake(runID: string, frameID: string, waitID: string, operationID: string, facts: Json[]): Json {
  return copyJson({ encoding: "initial-wait-fact-witness-v1", runID, frameID, waitID, operationID, facts });
}
export function initialWaitSource(encoded: unknown): InitialWaitSource | null {
  const value = typeof encoded === "string" ? parseJson(encoded) : encoded;
  if (!object(value) || value.encoding !== "initial-wait-fact-witness-v1") return null;
  if (Object.keys(value).sort().join(",") !== "encoding,facts,frameID,operationID,runID,waitID" || !Array.isArray(value.facts)
      || ![value.runID, value.frameID, value.waitID, value.operationID].every(id => typeof id === "string" && id.length > 0)) invalid();
  return value as unknown as InitialWaitSource;
}
function sourceProof(store: SQLiteStore, runID: string, encoded: unknown): InitialWaitSource {
  const source = initialWaitSource(encoded); if (!source || source.runID !== runID) invalid();
  const wait = store.query(`SELECT w.* FROM waits w JOIN frames f ON f.id=w.frame_id AND f.run_id=w.run_id
    WHERE w.id=? AND w.run_id=? AND w.frame_id=? AND f.state='committed'`, [source.waitID, runID, source.frameID])[0];
  const receipt = store.query("SELECT type,subject_id,request_json,data_json FROM history WHERE operation_id=?", [source.operationID])[0];
  if (!wait || wait.winner !== "results" || wait.wake_json !== canonicalJson(source) || !receipt || receipt.type !== "core.suspend" || receipt.subject_id !== runID) invalid();
  const keys = parseJson(wait.action_keys_json as string);
  if (!Array.isArray(keys) || keys.length !== source.facts.length || keys.some((key, index) => { const fact = source.facts[index]; return !object(fact) || fact.actionKey !== key; })) invalid();
  const request = parseJson(receipt.request_json as string);
  if (!object(request) || request.waitID !== source.waitID || request.frameID !== source.frameID || canonicalJson(request.initialWake) !== canonicalJson(source)
      || !Array.isArray(request.actions) || !object(request.proposal) || request.proposal.operationID !== source.operationID || request.proposal.action !== "suspend") invalid();
  const rows = store.query("SELECT type,subject_id,data_projection_id FROM history WHERE operation_id IS NULL AND request_json=? ORDER BY sequence", [receipt.request_json as string]);
  const events = rows.map(row => ({ type: row.type as string, subjectID: row.subject_id as string, data: null,
    ...(row.data_projection_id === null ? {} : { dataProjectionID: row.data_projection_id as string }) })) as HistoryEvent[];
  const publications = Array.isArray(request.actionRequestValues) ? request.actionRequestValues.map(item => {
    if (!object(item) || !object(item.source) || !object(item.source.value)) invalid();
    return item.source.value as unknown as ValuePublication;
  }) : [];
  if (!store.matchesCommitEvidence({ operationID: source.operationID, kind: "core.suspend", subjectID: runID, request,
    writes: [], evidence: request.proposal, result: null, ...(publications.length ? { valuePublications: publications } : {}),
    ...(events.some(event => event.dataProjectionID !== undefined) ? { events } : {}) })) invalid();
  return source;
}
function witness(fact: Fact): Json {
  const hash = createHash("sha256"); let length = 0;
  for (const bytes of canonicalJsonChunks(fact.outcome)) { length += bytes.length; hash.update(bytes); }
  return { factID: fact.id, actionID: fact.actionID, actionKey: fact.actionKey, encoding: "canonical-json-utf8-v1", length, digest: hash.digest("hex") };
}
function factRow(store: SQLiteStore, runID: string, expected: Json, seen: Set<string>): Row {
  if (!object(expected) || Object.keys(expected).sort().join(",") !== "actionID,actionKey,digest,encoding,factID,length"
      || expected.encoding !== "canonical-json-utf8-v1" || typeof expected.factID !== "string" || typeof expected.actionID !== "string" || typeof expected.actionKey !== "string"
      || typeof expected.length !== "number" || !Number.isSafeInteger(expected.length) || expected.length <= 0 || typeof expected.digest !== "string" || !/^[a-f0-9]{64}$/.test(expected.digest)
      || seen.has(expected.factID)) invalid();
  seen.add(expected.factID);
  const row = store.query(`SELECT f.id,f.action_id,f.outcome_json,f.outcome_projection_id,a.action_key FROM facts f JOIN actions a ON a.id=f.action_id
    WHERE f.id=? AND f.action_id=? AND f.run_id=? AND a.run_id=?`, [expected.factID, expected.actionID, runID, runID])[0];
  if (!row || row.action_key !== expected.actionKey) invalid(); return row;
}
/** The immutable Core suspension receipt, retained wait and original Facts
 * jointly prove this value. A digest alone never authorizes a source. */
export function readInitialWaitWake(store: SQLiteStore, runID: string, encoded: unknown, evidence = false): Extract<Wake, { kind: "results" }> {
  const source = sourceProof(store, runID, encoded), facts: Fact[] = [], seen = new Set<string>();
  for (const expected of source.facts) {
    const row = factRow(store, runID, expected, seen);
    const fact: Fact = { id: row.id as string, actionID: row.action_id as string, actionKey: row.action_key as string,
      outcome: readProjectedJsonSync(store, row.outcome_json, row.outcome_projection_id, row.action_id as string, evidence) as Fact["outcome"] };
    if (!object(fact.outcome) || !["succeeded", "failed", "cancelled"].includes(fact.outcome.kind as string)) invalid();
    if (canonicalJson(witness(fact)) !== canonicalJson(expected)) invalid(); facts.push(fact);
  }
  return { kind: "results", waitID: source.waitID, facts };
}
export async function readInitialWaitWakeAsync(store: SQLiteStore, runID: string, encoded: unknown, evidence = false): Promise<Extract<Wake, { kind: "results" }>> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority(); assert(); const epoch = store.coordinatorEpoch;
  const source = sourceProof(store, runID, encoded), facts: Fact[] = [], seen = new Set<string>();
  for (const expected of source.facts) {
    const row = factRow(store, runID, expected, seen);
    const fact: Fact = { id: row.id as string, actionID: row.action_id as string, actionKey: row.action_key as string,
      outcome: evidence ? await readFactOutcomeForEvidence(store, row) : await readFactOutcome(store, row) };
    assert(); if (store.coordinatorEpoch !== epoch) invalid();
    // The outcome's canonical witness is calculated incrementally, yielding
    // between bounded chunks rather than joining the complete wake string.
    const hash = createHash("sha256"); let length = 0;
    for (const bytes of canonicalJsonChunks(fact.outcome)) { hash.update(bytes); length += bytes.length; await new Promise<void>(resolve => setImmediate(resolve)); assert(); if (store.coordinatorEpoch !== epoch) invalid(); }
    if (canonicalJson({ factID: fact.id, actionID: fact.actionID, actionKey: fact.actionKey, encoding: "canonical-json-utf8-v1", length, digest: hash.digest("hex") }) !== canonicalJson(expected)) invalid();
    facts.push(fact);
  }
  sourceProof(store, runID, encoded); assert(); if (store.coordinatorEpoch !== epoch) invalid();
  return { kind: "results", waitID: source.waitID, facts };
}
