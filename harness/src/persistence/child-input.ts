import { canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, type Json, type RunView } from "../contract/index.js";
import { readFixedActionRequest, readFixedActionRequestAsync } from "./fixed-action-request.js";
import type { SQLiteStore } from "./sqlite.js";
type Obj = Record<string, Json>;
const object = (v: unknown): v is Obj => !!v && typeof v === "object" && !Array.isArray(v);
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid child Run input source"); }
export function encodeChildInput(value: unknown): Json {
  const body = copyJson(value); if (!object(body)) invalid();
  if (body.inputSource !== undefined || typeof body.parentActionID !== "string") return body;
  const fields = { input: body.input!, config: body.config! }; let size = 0;
  for (const part of canonicalJsonChunks(fields)) { size += part.length; if (size > 16_384) break; }
  if (size <= 16_384) return body;
  return { ...body, input: null, config: null, inputSource: { encoding: "parent-action-input-v1", actionID: body.parentActionID, digest: canonicalJsonDigest(fields) } };
}
export function encodeChildFrame(value: unknown): Json {
  const input = copyJson(value); if (!object(input) || !object(input.run)) invalid();
  return { ...input, run: encodeChildInput(input.run) };
}
export function hasChildFrameSource(value: unknown): boolean {
  const input = typeof value === "string" ? parseJson(value) : value;
  return object(input) && object(input.run) && (input.run.inputSource !== undefined || input.continuationSource !== undefined);
}
/** Metadata validation can run before reading payloads or granting access. */
export function childInputBinding(store: SQLiteStore, value: Obj): { actionID: string; requestJSON: string; digest: string } | null {
  if (value.inputSource === undefined) return null;
  const source = value.inputSource;
  if (!object(source) || Object.keys(source).sort().join(",") !== "actionID,digest,encoding" || source.encoding !== "parent-action-input-v1"
      || typeof value.id !== "string" || typeof source.actionID !== "string" || source.actionID !== value.parentActionID
      || typeof source.digest !== "string" || !/^[a-f0-9]{64}$/.test(source.digest) || value.input !== null || value.config !== null) invalid();
  const row = store.query(`SELECT a.request_json,p.snapshot_json parent_json,c.principal_id child_principal,p.principal_id parent_principal
    FROM remote_run_links l JOIN actions a ON a.id=l.action_id JOIN runs c ON c.id=l.child_run_id JOIN runs p ON p.id=a.run_id
    WHERE l.action_id=? AND l.child_run_id=? AND c.parent_action_id=a.id`, [source.actionID, value.id])[0];
  if (!row || row.child_principal !== row.parent_principal) invalid();
  const request = parseJson(row.request_json as string) as Obj, parent = parseJson(row.parent_json as string) as Obj;
  if (request.kind !== "agent" || canonicalJsonDigest(request.target) !== canonicalJsonDigest(value.agent) || parent.rootID !== value.rootID) invalid();
  const fixed = request.valueSource as Obj | undefined, ref = fixed?.value as Obj | undefined;
  const digest = ref?.digest ?? canonicalJsonDigest({ input: request.input, config: request.config });
  if (digest !== source.digest) invalid();
  return { actionID: source.actionID, requestJSON: row.request_json as string, digest: source.digest };
}
function restore(body: Obj, request: { input: Json; config: Json }, digest: string): Json {
  if (canonicalJsonDigest({ input: request.input, config: request.config }) !== digest) invalid();
  const { inputSource: _source, ...value } = body;
  return { ...value, input: request.input, config: request.config };
}
export function readChildInput(store: SQLiteStore, value: unknown): Json {
  if (!object(value)) invalid();
  const source = childInputBinding(store, value); if (!source) return value;
  return restore(value, readFixedActionRequest(store, source.actionID, source.requestJSON), source.digest);
}
export async function readChildInputAsync(store: SQLiteStore, value: unknown, evidence = false): Promise<Json> {
  if (!object(value)) invalid();
  const source = childInputBinding(store, value); if (!source) return value;
  return restore(value, await readFixedActionRequestAsync(store, source.actionID, source.requestJSON, evidence), source.digest);
}
export function readChildFrame(store: SQLiteStore, value: unknown): Json {
  if (!object(value)) invalid(); return { ...value, run: readChildInput(store, value.run) };
}
export async function readChildFrameAsync(store: SQLiteStore, value: unknown, evidence = false): Promise<Json> {
  if (!object(value)) invalid(); return { ...value, run: await readChildInputAsync(store, value.run, evidence) };
}
export function childAcceptanceEvent(child: RunView, principalID: string): { type: string; subjectID: string; data: Json } {
  const value = encodeChildInput(child) as Obj;
  return { type: "run.accepted", subjectID: child.id, data: copyJson({ runID: child.id, rootID: child.rootID, parentActionID: child.parentActionID,
    principalID, agent: child.agent, input: value.input, config: value.config, limits: child.limits, ...(value.inputSource ? { inputSource: value.inputSource } : {}) }) };
}
export function sameChildValue(a: unknown, b: unknown): boolean {
  const x = canonicalJsonChunks(a)[Symbol.iterator](), y = canonicalJsonChunks(b)[Symbol.iterator]();
  for (;;) { const p = x.next(), q = y.next(); if (p.done || q.done) return p.done === q.done; if (!Buffer.from(p.value).equals(q.value)) return false; }
}
