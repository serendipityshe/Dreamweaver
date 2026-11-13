import { canonicalJson, canonicalJsonChunks, copyJson, parseJson, type Fact } from "../contract/index.js";
import { randomUUID } from "node:crypto";
import { frameResultProjectionWrites, reportProjectionWrites, factHistoryValue, factHistoryMatches, type HistoryEvent, type SQLiteStore, type SQLWrite } from "../persistence/index.js";

function fitsBytes(value: unknown, maxBytes: number): boolean {
  let bytes = 0;
  for (const chunk of canonicalJsonChunks(value)) { bytes += chunk.byteLength; if (bytes > maxBytes) return false; }
  return true;
}

/** Exact comparison to an existing SQLite text value without constructing a
 * second whole JSON string or conflating UTF-8 bytes with UTF-16 units. */
function sameCanonicalText(text: unknown, value: unknown): boolean {
  if (typeof text !== "string") return false;
  const expected = canonicalJsonChunks(value)[Symbol.iterator]();
  let next = expected.next(), offset = 0;
  for (let start = 0; start < text.length;) {
    let end = Math.min(start + 8192, text.length);
    const high = text.charCodeAt(end - 1), low = text.charCodeAt(end);
    if (end < text.length && high >= 0xd800 && high <= 0xdbff && low >= 0xdc00 && low <= 0xdfff) end--;
    const actual = Buffer.from(text.slice(start, end), "utf8"); let position = 0;
    while (position < actual.length) {
      if (next.done) return false;
      const length = Math.min(actual.length - position, next.value.length - offset);
      if (!actual.subarray(position, position + length).equals(Buffer.from(next.value.buffer, next.value.byteOffset + offset, length))) return false;
      position += length; offset += length;
      if (offset === next.value.length) { next = expected.next(); offset = 0; }
    }
    start = end;
  }
  return next.done === true;
}

/** Preserve all facts in ordered append-only events when the combined public
 * history record would not fit; the actual wake and fact rows stay unchanged. */
export function waitResolutionEvents(store: SQLiteStore, runID: string, waitID: string, facts: Fact[], maxResponseBytes?: number, pendingActionKeys?: string[]): HistoryEvent[] {
  const event = (type: string, data: unknown): HistoryEvent => ({ type, subjectID: runID, data: copyJson(data) });
  const winner = pendingActionKeys === undefined ? "results" : "timeout";
  const data = { waitID, winner, facts, ...(pendingActionKeys === undefined ? {} : { pendingActionKeys }) };
  if (maxResponseBytes === undefined) return [event("wait.resolved", data)];
  const owner = store.query("SELECT principal_id FROM runs WHERE id=?", [runID])[0]?.principal_id;
  if (typeof owner !== "string") return [event("wait.resolved", data)];
  const max = Number.MAX_SAFE_INTEGER;
  const cursor = Buffer.from(canonicalJson({ high: max, position: max })).toString("base64url") + "." + "A".repeat(43);
  const record = { id: "00000000-0000-0000-0000-000000000000", sequence: max, at: max, runID, actionID: null, attemptID: null, type: "wait.resolved", data };
  const envelope = { protocolVersion: "harness/1", requestID: "00000000-0000-0000-0000-000000000000",
    binding: { domainID: store.domainID, principalID: owner }, ok: true, result: { items: [record], nextCursor: cursor } };
  if (fitsBytes(envelope, maxResponseBytes)) return [event("wait.resolved", data)];
  return [event("wait.resolved", { waitID, winner, factCount: facts.length, representation: "events", ...(pendingActionKeys === undefined ? {} : { pendingCount: pendingActionKeys.length }) }),
    ...facts.map((fact, index) => event("wait.fact", { waitID, index, factCount: facts.length, fact })),
    ...(pendingActionKeys ?? []).map((actionKey, index) => event("wait.pending", { waitID, index, pendingCount: pendingActionKeys!.length, actionKey }))];
}

/** A referenced Fact is already backed by the original report. Preserve the
 * ordinary ordered history API while making that event point at the same
 * immutable report instead of serializing its outcome again. */
export function referencedWaitResolutionEvents(store: SQLiteStore, runID: string, waitID: string, facts: Fact[], maxResponseBytes?: number, pendingActionKeys?: string[], operationID = `wait.resolve:${waitID}:${pendingActionKeys === undefined ? "results" : "timeout"}`): { events: HistoryEvent[]; writes: SQLWrite[] } {
  const sources = facts.map(fact => store.query(`SELECT b.report_id FROM facts f JOIN value_projection_bindings b ON b.projection_id=f.outcome_projection_id
    WHERE f.id=? AND f.action_id=? AND f.run_id=? AND b.ordinal=0 AND b.path_json='[]' AND b.selector='result'`, [fact.id, fact.actionID, runID])[0]
    ?? store.query(`SELECT p.* FROM facts f JOIN frame_result_projections p ON p.projection_id=f.outcome_projection_id
      WHERE f.id=? AND f.action_id=? AND f.run_id=? AND p.paths_json='[[]]'`, [fact.id, fact.actionID, runID])[0]);
  if (!sources.some(Boolean) && fitsBytes(facts, 16_384)) return { events: waitResolutionEvents(store, runID, waitID, facts, maxResponseBytes, pendingActionKeys), writes: [] };
  const winner = pendingActionKeys === undefined ? "results" : "timeout";
  const events: HistoryEvent[] = [{ type: "wait.resolved", subjectID: runID, data: copyJson({ waitID, winner, factCount: facts.length,
    representation: "events", ...(pendingActionKeys === undefined ? {} : { pendingCount: pendingActionKeys.length }) }) }];
  const writes: SQLWrite[] = [];
  for (const [index, fact] of facts.entries()) {
    const source = sources[index];
    if (!source) { events.push({ type: "wait.fact", subjectID: runID, data: factHistoryValue(waitID, index, facts.length, fact, operationID) }); continue; }
    const id = randomUUID(), template = copyJson({ waitID, index, factCount: facts.length, fact: { id: fact.id, actionID: fact.actionID, actionKey: fact.actionKey, outcome: null } });
    if (typeof source.report_id === "string") writes.push(...reportProjectionWrites(id, fact.actionID, template, [{ reportID: source.report_id, path: ["fact", "outcome"], selector: "result" }]));
    else if (typeof source.frame_id === "string" && typeof source.run_id === "string" && typeof source.operation_id === "string" && typeof source.digest === "string")
      writes.push(...frameResultProjectionWrites(id, fact.actionID, template, [["fact", "outcome"]], { frameID: source.frame_id, runID: source.run_id, operationID: source.operation_id, digest: source.digest }));
    else throw new Error("Wait fact source is unavailable");
    events.push({ type: "wait.fact", subjectID: runID, data: null, dataProjectionID: id });
  }
  for (const [index, actionKey] of (pendingActionKeys ?? []).entries())
    events.push({ type: "wait.pending", subjectID: runID, data: copyJson({ waitID, index, pendingCount: pendingActionKeys!.length, actionKey }) });
  return { events, writes };
}

/** Both the old single event and the new complete ordered event series are
 * valid proofs. Never accept a partial or reordered series. */
export function hasWaitResolutionEvents(store: SQLiteStore, runID: string, waitID: string, facts: Fact[], pendingActionKeys?: string[]): boolean {
  const rows = store.query("SELECT sequence,type,data_json,data_projection_id FROM history WHERE subject_id=? AND type IN ('wait.resolved','wait.fact','wait.pending') ORDER BY sequence", [runID]);
  const winner = pendingActionKeys === undefined ? "results" : "timeout";
  const full = { waitID, winner, facts, ...(pendingActionKeys === undefined ? {} : { pendingActionKeys }) };
  const summary = { waitID, winner, factCount: facts.length, representation: "events", ...(pendingActionKeys === undefined ? {} : { pendingCount: pendingActionKeys.length }) };
  return rows.some((row, start) => {
    if (row.type !== "wait.resolved") return false;
    if (sameCanonicalText(row.data_json, full)) return true;
    if (!sameCanonicalText(row.data_json, summary)) return false;
    const factEventsMatch = facts.every((fact, index) => {
      const next = rows[start + 1 + index];
      if (next?.type !== "wait.fact") return false;
      if (next.data_projection_id == null) {
        const data = parseJson(next.data_json as string);
        if (data && typeof data === "object" && !Array.isArray(data) && data.encoding === "wait-fact-history-v1")
          return factHistoryMatches(store, runID, data, waitID, index, facts.length, fact);
        return sameCanonicalText(next.data_json, { waitID, index, factCount: facts.length, fact });
      }
      const source = store.query(`SELECT p.template_json FROM value_projections p JOIN value_projection_bindings b ON b.projection_id=p.id
        JOIN facts f ON f.id=? AND f.action_id=p.action_id AND f.run_id=? JOIN value_projection_bindings fb ON fb.projection_id=f.outcome_projection_id
        WHERE p.id=? AND p.action_id=? AND p.binding_count=1 AND b.ordinal=0 AND b.path_json='["fact","outcome"]' AND b.selector='result'
          AND fb.ordinal=0 AND fb.path_json='[]' AND fb.selector='result' AND fb.report_id=b.report_id`, [fact.id, runID, next.data_projection_id as string, fact.actionID])[0]
        ?? store.query(`SELECT p.template_json FROM value_projections p JOIN frame_result_projections b ON b.projection_id=p.id
          JOIN facts f ON f.id=? AND f.action_id=p.action_id AND f.run_id=? JOIN frame_result_projections fb ON fb.projection_id=f.outcome_projection_id
          WHERE p.id=? AND p.action_id=? AND b.paths_json='[["fact","outcome"]]' AND fb.paths_json='[[]]'
            AND b.run_id=fb.run_id AND b.frame_id=fb.frame_id AND b.operation_id=fb.operation_id AND b.digest=fb.digest`, [fact.id, runID, next.data_projection_id as string, fact.actionID])[0];
      return next.data_json === "null" && !!source && sameCanonicalText(source.template_json, { waitID, index, factCount: facts.length,
        fact: { id: fact.id, actionID: fact.actionID, actionKey: fact.actionKey, outcome: null } });
    });
    return factEventsMatch && (pendingActionKeys ?? []).every((actionKey, index) => {
      const next = rows[start + 1 + facts.length + index];
      return next?.type === "wait.pending" && sameCanonicalText(next.data_json, { waitID, index, pendingCount: pendingActionKeys!.length, actionKey });
    });
  });
}
