import { copyJson } from "./json.js";
import type { Json } from "./types.js";
import { createHash } from "node:crypto";

// At most 8192 UTF-16 code units are encoded at once. A code unit can expand
// to six ASCII bytes in JSON, so each produced Buffer is below 64 KiB.
const STRING_UNITS = 8192;
const CHUNK_BYTES = 65_536;
type Task =
  | { kind: "value"; value: Json }
  | { kind: "array"; value: Json[]; index: number }
  | { kind: "object"; value: { [key: string]: Json }; keys: string[]; index: number };

/**
 * Emit exactly the canonical JSON UTF-8 encoding without assembling the whole
 * encoding into one JavaScript string or Buffer. The returned sequence owns a
 * validated snapshot, so callers may await between chunks without observing a
 * later caller mutation. A single native string is still limited by the host.
 */
export function canonicalJsonChunks(input: unknown): Iterable<Uint8Array> {
  const snapshot = copyJson(input);
  return { [Symbol.iterator]: () => pack(encode(snapshot)) };
}

/** Digest the same canonical UTF-8 bytes as canonicalJson without assembling
 * the full encoded document. This is not, by itself, proof of a commit. */
export function canonicalJsonDigest(input: unknown): string {
  const hash = createHash("sha256");
  for (const bytes of canonicalJsonChunks(input)) hash.update(bytes);
  return hash.digest("hex");
}

function* pack(parts: Iterable<Uint8Array>): Generator<Uint8Array> {
  let frame = Buffer.allocUnsafe(CHUNK_BYTES), used = 0;
  for (const part of parts) {
    let offset = 0;
    while (offset < part.byteLength) {
      const count = Math.min(CHUNK_BYTES - used, part.byteLength - offset);
      frame.set(part.subarray(offset, offset + count), used);
      used += count; offset += count;
      if (used === CHUNK_BYTES) { yield frame; frame = Buffer.allocUnsafe(CHUNK_BYTES); used = 0; }
    }
  }
  if (used) yield frame.subarray(0, used);
}

function* stringChunks(value: string): Generator<Uint8Array> {
  yield Buffer.from('"');
  for (let start = 0; start < value.length;) {
    let end = Math.min(start + STRING_UNITS, value.length);
    // JSON.stringify must see a valid pair together, including at a chunk
    // boundary. Lone surrogates remain escaped exactly as in JSON.stringify.
    const high = value.charCodeAt(end - 1), low = value.charCodeAt(end);
    if (end < value.length && high >= 0xd800 && high <= 0xdbff && low >= 0xdc00 && low <= 0xdfff) end--;
    const encoded = JSON.stringify(value.slice(start, end));
    yield Buffer.from(encoded.slice(1, -1), "utf8");
    start = end;
  }
  yield Buffer.from('"');
}

function* encode(value: Json): Generator<Uint8Array> {
  const tasks: Task[] = [{ kind: "value", value }];
  while (tasks.length) {
    const task = tasks.pop()!;
    if (task.kind === "value") {
      const item = task.value;
      if (typeof item === "string") { yield* stringChunks(item); continue; }
      if (item === null || typeof item !== "object") { yield Buffer.from(JSON.stringify(item), "utf8"); continue; }
      if (Array.isArray(item)) { yield Buffer.from("["); tasks.push({ kind: "array", value: item, index: 0 }); }
      else { yield Buffer.from("{"); tasks.push({ kind: "object", value: item, keys: Object.keys(item).sort(), index: 0 }); }
      continue;
    }
    if (task.kind === "array") {
      if (task.index === task.value.length) { yield Buffer.from("]"); continue; }
      if (task.index) yield Buffer.from(",");
      const item = task.value[task.index++]!;
      tasks.push(task, { kind: "value", value: item });
      continue;
    }
    if (task.index === task.keys.length) { yield Buffer.from("}"); continue; }
    if (task.index) yield Buffer.from(",");
    const key = task.keys[task.index++]!;
    yield* stringChunks(key);
    yield Buffer.from(":");
    tasks.push(task, { kind: "value", value: task.value[key]! });
  }
}
