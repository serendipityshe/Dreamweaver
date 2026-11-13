import { canonicalJson, canonicalJsonDigest, copyJson, harnessError, parseJson, type Fact, type Json } from "../contract/index.js";
import { readFactOutcome, readProjectedJsonSync } from "./projection.js";
import { commandProjectionByteBound } from "./command-value.js";
import type { SQLiteStore } from "./sqlite.js";

type Obj = Record<string, Json>;
const object = (v: unknown): v is Obj => !!v && typeof v === "object" && !Array.isArray(v);
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Wait fact history source is unavailable or inconsistent"); }
export function factHistoryValue(waitID: string, index: number, factCount: number, fact: Fact, operationID: string): Json {
  return { encoding: "wait-fact-history-v1", waitID, index, factCount, operationID, outcomeDigest: canonicalJsonDigest(fact.outcome),
    fact: { id: fact.id, actionID: fact.actionID, actionKey: fact.actionKey, outcome: null } };
}
function binding(store: SQLiteStore, runID: string, value: Json) {
  if (!object(value) || value.encoding !== "wait-fact-history-v1" || typeof value.waitID !== "string" || typeof value.operationID !== "string"
      || !Number.isSafeInteger(value.index) || !Number.isSafeInteger(value.factCount) || (value.index as number) < 0 || (value.factCount as number) <= (value.index as number)
      || typeof value.outcomeDigest !== "string" || !/^[a-f0-9]{64}$/.test(value.outcomeDigest)
      || !object(value.fact) || typeof value.fact.id !== "string" || typeof value.fact.actionID !== "string" || typeof value.fact.actionKey !== "string" || value.fact.outcome !== null) invalid();
  const row = store.query(`SELECT f.id,f.action_id,f.outcome_json,f.outcome_projection_id,a.action_key FROM facts f JOIN actions a ON a.id=f.action_id
    JOIN waits w ON w.id=? AND w.run_id=f.run_id WHERE f.id=? AND f.action_id=? AND f.run_id=? AND a.run_id=?`,
    [value.waitID, value.fact.id, value.fact.actionID, runID, runID])[0];
  if (!row || row.action_key !== value.fact.actionKey) invalid();
  const proof = store.query(`SELECT h.request_json FROM history h JOIN history e ON e.request_json=h.request_json AND e.subject_id=h.subject_id
    WHERE h.operation_id=? AND h.subject_id=? AND h.type IN ('wait.resolve','core.suspend') AND e.type='wait.fact' AND e.data_json=?`,
    [value.operationID, runID, canonicalJson(value)])[0];
  if (!proof) invalid();
  const request = parseJson(proof.request_json as string);
  if (!object(request) || request.waitID !== value.waitID) invalid();
  return { value, row };
}
function restore(value: Obj, outcome: Json): Json {
  if (canonicalJsonDigest(outcome) !== value.outcomeDigest || !object(value.fact)) invalid();
  return { waitID: value.waitID!, index: value.index!, factCount: value.factCount!, fact: { ...value.fact, outcome } };
}
export function readFactHistory(store: SQLiteStore, runID: string, value: Json): Json {
  const source = binding(store, runID, value);
  return restore(source.value, readProjectedJsonSync(store, source.row.outcome_json, source.row.outcome_projection_id, source.row.action_id as string));
}
export async function readFactHistoryAsync(store: SQLiteStore, runID: string, value: Json): Promise<Json> {
  const source = binding(store, runID, value);
  return restore(source.value, copyJson(await readFactOutcome(store, source.row)));
}
/** Evidence already reconstructed the fixed fact set. Compare the immutable
 * history binding, rather than reading the same large outcome for each check. */
export function factHistoryMatches(store: SQLiteStore, runID: string, value: Json, waitID: string, index: number, factCount: number, fact: Fact): boolean {
  const source = binding(store, runID, value), expected = factHistoryValue(waitID, index, factCount, fact, source.value.operationID as string);
  return canonicalJson(value) === canonicalJson(expected);
}
export function factHistoryByteBound(store: SQLiteStore, runID: string, value: Json): number {
  const source = binding(store, runID, value);
  if (source.row.outcome_projection_id !== null) invalid(); // Existing report/frame projections have their own history representation.
  return commandProjectionByteBound(store, source.row.outcome_json) + Buffer.byteLength(canonicalJson(value));
}
