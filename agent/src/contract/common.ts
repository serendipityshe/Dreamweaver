import { createHash } from "node:crypto";
import { isProxy } from "node:util/types";
import { HarnessError, type ErrorCode, type Json, type Ref } from "../../../harness/src/index.js";

export function fail(message: string, code: ErrorCode = "INVALID_ARGUMENT"): never {
  throw new HarnessError({ code, message, disposition: "rejected", details: null, retryAfterMs: null });
}
// Agent data is bounded by Harness at admission. Reject accessors and non-JSON
// values here rather than invoking user code while compiling a definition.
export function json(value: unknown, ancestors = new Set<object>()): Json {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER) return Object.is(value, -0) ? 0 : value;
  if (!value || typeof value !== "object" || isProxy(value) || ancestors.has(value)) return fail("Expected acyclic Json");
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      if (Object.getPrototypeOf(value) !== Array.prototype || Reflect.ownKeys(value).length !== value.length + 1) fail("Sparse or extended arrays are not Json");
      return Array.from({ length: value.length }, (_, i) => {
        const d = Object.getOwnPropertyDescriptor(value, String(i));
        if (!d || !("value" in d) || !d.enumerable) fail("Accessors are not Json");
        return json(d.value, ancestors);
      });
    }
    if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) fail("Expected plain Json object");
    const result: Record<string, Json> = {};
    for (const key of Reflect.ownKeys(value).sort((a, b) => String(a).localeCompare(String(b)))) {
      const d = Object.getOwnPropertyDescriptor(value, key)!;
      if (typeof key !== "string" || !("value" in d) || !d.enumerable) fail("Expected enumerable Json data");
      Object.defineProperty(result, key, { value: json(d.value, ancestors), enumerable: true, writable: true, configurable: true });
    }
    return result;
  } finally { ancestors.delete(value); }
}
export function canonical(value: unknown): string {
  const v = json(value);
  const encode = (x: Json): string => x && typeof x === "object" ? Array.isArray(x) ? `[${x.map(encode).join(",")}]` : `{${Object.keys(x).sort().map(k => `${JSON.stringify(k)}:${encode(x[k]!)}`).join(",")}}` : JSON.stringify(x);
  return encode(v);
}
export const digest = (value: unknown): string => createHash("sha256").update(canonical(value)).digest("hex");
export const copy = <T>(value: T): T => json(value) as T;
export function freeze<T>(value: T): T {
  if (value && typeof value === "object") { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
  return value;
}
export function fields(value: unknown, allowed: string[], required: string[] = []): asserts value is Record<string, any> {
  if (!value || typeof value !== "object" || isProxy(value) || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail("Expected an options object");
  for (const key of Reflect.ownKeys(value)) {
    const d = Object.getOwnPropertyDescriptor(value, key)!;
    if (typeof key !== "string" || !allowed.includes(key) || !("value" in d) || !d.enumerable) fail(`Unknown or invalid field: ${String(key)}`);
  }
  for (const key of required) if (!Object.hasOwn(value, key)) fail(`Missing field: ${key}`);
}
export function string(value: unknown): asserts value is string { if (typeof value !== "string" || !value.length || value.trim() !== value) fail("Expected nonempty string"); }
export function positive(value: unknown): asserts value is number { if (!Number.isSafeInteger(value) || (value as number) <= 0) fail("Expected positive safe integer"); }
export function ref(value: unknown, reserved = true): asserts value is Ref {
  fields(value, ["id", "version"], ["id", "version"]); string(value.id); string(value.version);
  if (reserved && value.id.startsWith("dw.generated.")) fail("Reserved definition ID");
}
export const equal = (a: unknown, b: unknown): boolean => canonical(a) === canonical(b);
export const generated = (kind: string, binding: unknown): Ref => ({ id: `dw.generated.${kind}`, version: digest(binding) });
