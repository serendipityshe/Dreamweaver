import type { Json } from "../../../harness/src/index.js";
import { fields, fail, ref, string } from "./common.js";
import { messages } from "../../../capacities/model/contract/validation.js";

export function contextResult(value: unknown): void {
  fields(value, ["messages", "sources"], ["messages", "sources"]); messages(value.messages);
  if (!Array.isArray(value.sources)) fail("Invalid context sources");
  for (const source of value.sources) {
    fields(source, ["resolver", "scope", "key", "revision", "sha256", "mediaType", "bytes"], ["resolver", "scope", "key", "revision", "sha256", "mediaType", "bytes"]);
    ref(source.resolver, false); for (const key of ["scope", "key", "revision", "mediaType"]) string(source[key]);
    if (!/^[0-9a-f]{64}$/.test(source.sha256) || !Number.isSafeInteger(source.bytes) || source.bytes < 0) fail("Invalid immutable context reference");
  }
}
