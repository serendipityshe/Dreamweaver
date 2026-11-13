// Shared restricted-JSON boundary used by the five runtime modules.
import { isProxy } from "node:util/types";
import { harnessError } from "./errors.js";
import type { Json } from "./types.js";
import { canonicalJsonChunks } from "./json-chunks.js";

function invalid(message: string): never {
  throw harnessError("INVALID_ARGUMENT", message);
}

function numberValue(value: number, reject: (message: string) => never = invalid): number {
  if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) {
    reject("JSON numbers must be finite binary64 values with safe integers");
  }
  return Object.is(value, -0) ? 0 : value;
}

const copyValidationErrors = new WeakSet<object>();
let wholeJsonTestLimit: number | undefined;

/** Test-only structural guard. It does not change the public value limit. */
export function setWholeJsonLimitForTest(limit: number): () => void {
  if (!Number.isSafeInteger(limit) || limit <= 0 || wholeJsonTestLimit !== undefined) throw new Error("Invalid whole JSON test limit");
  wholeJsonTestLimit = limit;
  return () => { wholeJsonTestLimit = undefined; };
}
/** Only errors established by the current copy's own validators are branded. */
export function isCopyJsonValidationError(error: unknown): boolean {
  return typeof error === "object" && error !== null && copyValidationErrors.has(error);
}

/** Validate and copy plain data without invoking accessors or toJSON. */
export function copyJson(value: unknown): Json {
  const localErrors = new WeakSet<object>();
  function invalidCopy(message: string): never {
    const error = harnessError("INVALID_ARGUMENT", message); localErrors.add(error); throw error;
  }
  const active = new Set<object>();
  type Frame = { item: object; result: Json[] | { [key: string]: Json }; descriptors: PropertyDescriptorMap; keys: string[]; array: boolean; length: number; index: number };
  const stack: Frame[] = [];
  function enter(item: unknown): Json {
    if (item === null || typeof item === "string" || typeof item === "boolean") return item;
    if (typeof item === "number") return numberValue(item, invalidCopy);
    if (typeof item !== "object" || isProxy(item)) invalidCopy("Expected restricted JSON data");
    if (active.has(item)) invalidCopy("Cyclic JSON data");
    const array = Array.isArray(item);
    const proto = Object.getPrototypeOf(item);
    if (array ? proto !== Array.prototype : proto !== Object.prototype && proto !== null) {
      invalidCopy("JSON objects must be plain data objects");
    }
    active.add(item);
    const descriptors = Object.getOwnPropertyDescriptors(item);
    if (Reflect.ownKeys(item).some(key => typeof key !== "string")) invalidCopy("JSON symbols are not supported");
    const keys = Object.keys(descriptors);
    if (array) {
      const length = (item as unknown[]).length;
      if (keys.length !== length + 1) invalidCopy("Sparse or extended JSON arrays are not supported");
      const result: Json[] = [];
      stack.push({ item, result, descriptors, keys, array: true, length, index: 0 });
      return result;
    }
    const result: { [key: string]: Json } = {};
    stack.push({ item, result, descriptors, keys, array: false, length: keys.length, index: 0 });
    return result;
  }
  try {
    const result = enter(value);
    while (stack.length) {
    const frame = stack[stack.length - 1]!;
    if (frame.index === frame.length) { active.delete(frame.item); stack.pop(); continue; }
    const key = frame.array ? String(frame.index++) : frame.keys[frame.index++]!, descriptor = frame.descriptors[key];
    if (!descriptor || !("value" in descriptor) || !descriptor.enumerable)
      invalidCopy(frame.array ? "JSON arrays require plain elements" : "JSON objects require enumerable data properties");
    const child = enter(descriptor.value);
    if (frame.array) (frame.result as Json[]).push(child);
    else Object.defineProperty(frame.result, key, { value: child, enumerable: true, writable: true, configurable: true });
    }
    return result;
  } catch (error) {
    if (typeof error === "object" && error !== null && localErrors.has(error)) copyValidationErrors.add(error);
    else if (typeof error === "object" && error !== null && copyValidationErrors.has(error))
      throw new Error("JSON copy could not be verified", { cause: error });
    throw error;
  }
}

/** Parse before JSON.parse can discard duplicate decoded object keys. */
export function parseJson(source: string): Json {
  if (typeof source !== "string") invalid("JSON source must be a string");
  let position = 0;
  const space = () => { while (/^[\x20\t\r\n]$/.test(source[position] ?? "")) position++; };
  function string(): string {
    const start = position++;
    const marker = /["\\]/g;
    while (position < source.length) {
      // Skip ordinary runs in native string search. Large report strings can
      // otherwise monopolize the event loop while every character is read in
      // JavaScript; JSON.parse below still validates escapes and control chars.
      marker.lastIndex = position;
      const next = marker.exec(source);
      if (!next) invalid("Unterminated JSON string");
      if (next[0] === '"') {
        position = next.index + 1;
        try { return JSON.parse(source.slice(start, position)) as string; }
        catch { invalid("Invalid JSON string"); }
      }
      position = next.index + 2;
    }
    invalid("Unterminated JSON string");
  }
  function value(): Json {
    space();
    const char = source[position];
    if (char === '"') return string();
    if (char === "{") {
      position++; space();
      const result: { [key: string]: Json } = {};
      const keys = new Set<string>();
      if (source[position] === "}") { position++; return result; }
      while (true) {
        space();
        if (source[position] !== '"') invalid("Expected JSON object key");
        const key = string();
        if (keys.has(key)) invalid("Duplicate JSON object key");
        keys.add(key); space();
        if (source[position++] !== ":") invalid("Expected JSON colon");
        Object.defineProperty(result, key, { value: value(), enumerable: true, writable: true, configurable: true });
        space();
        const separator = source[position++];
        if (separator === "}") return result;
        if (separator !== ",") invalid("Expected JSON object separator");
      }
    }
    if (char === "[") {
      position++; space();
      const result: Json[] = [];
      if (source[position] === "]") { position++; return result; }
      while (true) {
        result.push(value()); space();
        const separator = source[position++];
        if (separator === "]") return result;
        if (separator !== ",") invalid("Expected JSON array separator");
      }
    }
    for (const [literal, parsed] of [["null", null], ["true", true], ["false", false]] as const) {
      if (source.startsWith(literal, position)) { position += literal.length; return parsed; }
    }
    const token = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(source.slice(position))?.[0];
    if (!token) invalid("Invalid JSON value");
    position += token.length;
    const parsed = numberValue(Number(token));
    if (parsed === 0 && /[1-9]/.test(token.split(/[eE]/)[0]!)) invalid("Nonzero JSON number underflow");
    return parsed;
  }
  try {
    const result = value(); space();
    if (position !== source.length) invalid("Unexpected trailing JSON data");
    return result;
  } catch (error) {
    if (error instanceof RangeError) invalid("JSON nesting is too deep");
    throw error;
  }
}

/** Stable structural encoding: decoded keys sorted, arrays ordered, -0 normalized. */
export function canonicalJson(value: unknown): string {
  function encode(item: Json): string {
    const encoded = item === null || typeof item !== "object" ? JSON.stringify(item)
      : Array.isArray(item) ? `[${item.map(encode).join(",")}]`
      : `{${Object.keys(item).sort().map(key => `${JSON.stringify(key)}:${encode(item[key]!)}`).join(",")}}`;
    if (wholeJsonTestLimit !== undefined && Buffer.byteLength(encoded, "utf8") > wholeJsonTestLimit)
      throw new Error("Whole JSON encoding exceeds the test-only limit");
    return encoded;
  }
  return encode(copyJson(value));
}

export function jsonEqual(left: unknown, right: unknown): boolean {
  const a = canonicalJsonChunks(left)[Symbol.iterator](), b = canonicalJsonChunks(right)[Symbol.iterator]();
  for (;;) {
    const x = a.next(), y = b.next();
    if (x.done || y.done) return x.done === y.done;
    if (!Buffer.from(x.value).equals(Buffer.from(y.value))) return false;
  }
}

/** Private value admission check; limits count canonical JSON UTF-8 bytes, including escapes. */
export function assertJsonValueSize(value: Json, maxBytes: number): void {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) invalid("Expected a positive safe maxValueBytes");
  let bytes = 0;
  for (const chunk of canonicalJsonChunks(value)) {
    bytes += chunk.length;
    if (bytes > maxBytes) throw harnessError("LIMIT_EXCEEDED", "JSON value exceeds maxValueBytes");
  }
}
