import { Ajv2020 } from "ajv/dist/2020.js";
import type { Json } from "../../../harness/src/index.js";
import { copy, fail, fields, json } from "./common.js";
export type JsonSchema = { [key: string]: Json };
const options = { strict: false, allErrors: false, coerceTypes: false, useDefaults: false, removeAdditional: false, validateFormats: false };
const ajv = new Ajv2020(options);
const keywords = ["type", "properties", "required", "additionalProperties", "items", "enum", "const", "anyOf", "oneOf", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "minLength", "maxLength", "minItems", "maxItems", "description"];
export function validator(schema: JsonSchema): (value: Json) => void {
  const snapshot = copy(schema);
  function check(s: unknown): void {
    fields(s, keywords);
    if (s.additionalProperties !== undefined && typeof s.additionalProperties !== "boolean") fail("additionalProperties must be boolean");
    if (s.properties !== undefined) { fields(s.properties, Object.keys(s.properties)); Object.values(s.properties).forEach(check); }
    if (s.items !== undefined) check(s.items);
    for (const key of ["anyOf", "oneOf"]) if (s[key] !== undefined) { if (!Array.isArray(s[key])) fail(`Invalid ${key}`); s[key].forEach(check); }
  }
  check(snapshot);
  let validate: ReturnType<Ajv2020["compile"]>;
  try {
    if (!ajv.validateSchema(snapshot)) fail("Invalid JSON Schema 2020-12 subset");
    // Ajv retains generated code even after removeSchema; keep compilation local.
    validate = new Ajv2020({ ...options, meta: false, validateSchema: false }).compile(snapshot);
  } catch { return fail("Invalid JSON Schema 2020-12 subset"); }
  return value => { json(value); if (!validate(value)) fail(`Schema mismatch: ${ajv.errorsText(validate.errors)}`); };
}
