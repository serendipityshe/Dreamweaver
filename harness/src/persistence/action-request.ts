import { canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, type ActionView, type Json } from "../contract/index.js";
import { readPreparationOriginals, readPreparationResolved } from "./preparation-values.js";
import type { SQLiteStore } from "./sqlite.js";
type Obj = { [key: string]: Json };
const obj = (v: unknown): v is Obj => !!v && typeof v === "object" && !Array.isArray(v);
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid Action request source"); }
const same = (a: unknown, b: unknown) => canonicalJsonDigest(a) === canonicalJsonDigest(b);
function large(value: unknown) { let size = 0; for (const part of canonicalJsonChunks(value)) { size += part.length; if (size > 16_384) return true; } return false; }
function slots(value: Obj, prepared: boolean): Obj { return { ...value, input: null, config: null, ...(prepared ? { policies: null } : {}) }; }

/** Keep routing metadata available; immutable input/config live in the accepted
 * preparation. The descriptor is private and must never escape as an ActionView. */
export function encodeActionRequests(input: unknown): Json {
  const value = copyJson(input); if (!obj(value) || !obj(value.original) || !obj(value.request)) invalid();
  if (Object.hasOwn(value, "requestSource")) invalid();
  if (!large([value.original, value.request])) return value;
  return { ...value, original: slots(value.original, false), request: slots(value.request, true),
    requestSource: { encoding: "action-requests-v1", digest: canonicalJsonDigest([value.original, value.request]) } };
}

export function actionAcceptanceEvent(action: ActionView, frameID: string) {
  const data = { runID: action.runID, frameID, actionID: action.id, original: copyJson(action.original), request: copyJson(action.request) };
  return { type: "action.accepted", subjectID: action.id, data: large([data.original, data.request])
    ? { encoding: "action-request-history-v1", runID: action.runID, frameID, actionID: action.id, key: action.original.key,
        digest: canonicalJsonDigest([action.original, action.request]) } as Json : data };
}

export function readActionAcceptance(store: SQLiteStore, actionID: string, encoded?: unknown): Obj {
  const events = store.query("SELECT data_json,request_json FROM history WHERE subject_id=? AND type='action.accepted'", [actionID]);
  if (events.length !== 1 || encoded !== undefined && encoded !== events[0]!.data_json) invalid();
  const event = parseJson(events[0]!.data_json as string); if (!obj(event)) invalid();
  if (event.encoding === undefined) return event;
  if (event.encoding !== "action-request-history-v1" || event.actionID !== actionID || typeof event.runID !== "string" || typeof event.frameID !== "string" || typeof event.key !== "string") invalid();
  const prep = store.query(`SELECT p.* FROM dispatch_preparations p JOIN frames f ON f.id=p.frame_id JOIN prepare_rounds r ON r.id=p.round_id
    JOIN history h ON h.subject_id=f.run_id AND h.type='core.suspend' AND h.request_json=?
    WHERE p.frame_id=? AND f.run_id=? AND f.state='committed' AND p.state='ready' AND r.state='accepted'
      AND json_extract(h.request_json,'$.frameID')=f.id`, [events[0]!.request_json as string, event.frameID as string, event.runID as string])[0];
  if (!prep) invalid();
  const originals = readPreparationOriginals(store, prep), resolved = readPreparationResolved(store, prep), index = originals.findIndex(a => a.key === event.key);
  if (index < 0 || !resolved[index] || resolved[index]!.key !== event.key
      || canonicalJsonDigest([originals[index], resolved[index]]) !== event.digest) invalid();
  return { runID: event.runID!, frameID: event.frameID!, actionID, original: copyJson(originals[index]), request: copyJson(resolved[index]) };
}

export function expandActionRequests(store: SQLiteStore, value: Json): Json {
  if (!obj(value) || !Object.hasOwn(value, "requestSource")) return value;
  const source = value.requestSource;
  if (!obj(source) || Object.keys(source).sort().join(",") !== "digest,encoding" || source.encoding !== "action-requests-v1" || typeof value.id !== "string") invalid();
  const event = readActionAcceptance(store, value.id as string);
  if (event.runID !== value.runID || !obj(event.original) || !obj(event.request)
      || canonicalJsonDigest([event.original, event.request]) !== source.digest
      || !same(value.original, slots(event.original, false)) || !same(value.request, slots(event.request, true))) invalid();
  const { requestSource: _source, ...view } = value;
  return { ...view, original: event.original, request: event.request };
}
