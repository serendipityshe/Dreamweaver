import { canonicalJsonDigest, copyJson, harnessError, type Json } from "../contract/index.js";
import { childInputBinding, encodeChildInput, readChildInput, readChildInputAsync } from "./child-input.js";
import { rootInputBinding, rootInputSource, rootFieldsSync, readSubmitFields, verifySubmitInput, type SubmitInputSource } from "./submit-input.js";
import type { SQLiteStore } from "./sqlite.js";
import { encodeContinuation, readContinuation, readContinuationAsync, type ContinuationSource } from "./continuation.js";
type Obj = Record<string, Json>;
const object = (v: unknown): v is Obj => !!v && typeof v === "object" && !Array.isArray(v);
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid Run input source"); }
/** Source selection stays private; public Run/frame values remain native JSON. */
export function encodeRunInput(store: SQLiteStore, value: unknown, explicit?: SubmitInputSource | null, continuation?: ContinuationSource): Json {
  const input = copyJson(value); if (!object(input)) invalid();
  const body = encodeContinuation(store, input, continuation);
  if (body.parentActionID !== null || body.inputSource !== undefined) return encodeChildInput(body);
  let source = explicit;
  if (source === undefined && typeof body.id === "string") {
    const binding = store.query("SELECT principal_id,request_key FROM root_run_inputs WHERE run_id=?", [body.id])[0];
    source = binding ? verifySubmitInput(store, binding.principal_id as string, binding.request_key as string) : null;
  }
  if (!source) return body;
  if (canonicalJsonDigest({ input: body.input, config: body.config }) !== source.value.digest) invalid();
  return { ...body, input: null, config: null, inputSource: rootInputSource(source) };
}
export function runInputBinding(store: SQLiteStore, value: Obj) {
  return rootInputBinding(store, value) ?? childInputBinding(store, value);
}
export function readRunInput(store: SQLiteStore, value: unknown): Json {
  if (!object(value)) invalid();
  const source = rootInputBinding(store, value);
  if (!source) return readContinuation(store, readChildInput(store, value) as Obj);
  const { inputSource: _source, ...body } = value;
  return readContinuation(store, { ...body, ...rootFieldsSync(store, source) });
}
export async function readRunInputAsync(store: SQLiteStore, value: unknown, evidence = false): Promise<Json> {
  if (!object(value)) invalid();
  const source = rootInputBinding(store, value);
  if (!source) return readContinuationAsync(store, await readChildInputAsync(store, value, evidence) as Obj, value.id, evidence);
  const { inputSource: _source, ...body } = value;
  return readContinuationAsync(store, { ...body, ...await readSubmitFields(store, source, evidence) }, value.id, evidence);
}
export function encodeRunFrame(store: SQLiteStore, value: unknown): Json {
  const body = copyJson(value); if (!object(body) || !object(body.run)) invalid();
  const { id: _id, ...encoded } = encodeContinuation(store, { ...body, id: body.run.id! });
  return { ...encoded, run: encodeRunInput(store, body.run) };
}
export function readRunFrame(store: SQLiteStore, value: unknown): Json {
  if (!object(value) || !object(value.run)) invalid(); return { ...readContinuation(store, value, value.run.id), run: readRunInput(store, value.run) };
}
export async function readRunFrameAsync(store: SQLiteStore, value: unknown, evidence = false): Promise<Json> {
  if (!object(value) || !object(value.run)) invalid(); return { ...await readContinuationAsync(store, value, value.run.id, evidence), run: await readRunInputAsync(store, value.run, evidence) };
}
