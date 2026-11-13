import { canonicalJsonChunks, canonicalJsonDigest } from "./json-chunks.js";
import { copyJson, parseJson } from "./json.js";
import { validateActionRequest, validateDecision } from "./validation.js";
import type { ActionRequest, Json } from "./types.js";

type ObjectJson = { [key: string]: Json };
const record = (value: unknown): value is ObjectJson => !!value && typeof value === "object" && !Array.isArray(value);
/** Private policy aliases retain routing Refs. The coordinator authenticates
 * their immutable registration source before making its bytes available. */
export function compactPolicyPayload(input: unknown, source?: Json): Json {
  const out = copyJson(input);
  if (!record(out) || !record(out.context) || !Array.isArray(out.context.policies)) return out;
  const context = out.context, currentPolicies = context.policies as Json[], selected = source ?? context.policySource;
  if (!record(selected) || selected.encoding !== "node-policies-v1" || !record(selected.value)) return out;
  if (context.policySource !== undefined && same(context.policySource, selected)
      && currentPolicies.every(p => record(p) && p.config === null)) return out;
  if (canonicalJsonDigest(currentPolicies) !== selected.value.digest) throw new Error("Policy source does not match its configuration");
  const policies = currentPolicies.map(p => {
    if (!record(p) || !record(p.ref)) throw new Error("Invalid policy metadata");
    return { ref: copyJson(p.ref), config: null };
  });
  if (record(context.prepared)) {
    if (!same(context.prepared.policies, context.policies)) throw new Error("Prepared policy binding differs");
    context.prepared.policies = copyJson(policies);
  }
  context.policies = policies; context.policySource = copyJson(selected); return out;
}
function same(left: unknown, right: unknown): boolean {
  const a = canonicalJsonChunks(left)[Symbol.iterator](), b = canonicalJsonChunks(right)[Symbol.iterator]();
  for (;;) {
    const x = a.next(), y = b.next();
    if (x.done || y.done) return x.done === y.done;
    if (!Buffer.from(x.value).equals(y.value)) return false;
  }
}

/** Pure preparation receives only the originals it checks. The coordinator
 * binds this compact context to the immutable candidate and original job;
 * the digest is not independent authorization or a commit receipt. */
export function compactDispatchPayload(input: unknown, force = false, compactValues = true): Json {
  input = compactPolicyPayload(input);
  if (!record(input) || !["rewrite", "admission"].includes(input.phase as string)) return copyJson(input);
  const context = input.context;
  if (!record(context)) throw new Error("Invalid dispatch context");
  let bytes = 0;
  for (const part of canonicalJsonChunks(context.decision)) { bytes += part.length; if (bytes > 4096) break; }
  if (!force && bytes <= 4096) return copyJson(input);
  const decision = validateDecision(context.decision);
  if (decision.kind !== "dispatch" || typeof context.frameID !== "string") throw new Error("Invalid dispatch source");
  const originals = input.phase === "rewrite" && Array.isArray(input.value) ? input.value.map(validateActionRequest) : [validateActionRequest(context.original)];
  const byKey = new Map(decision.actions.map(action => [action.key, action]));
  if (new Set(originals.map(action => action.key)).size !== originals.length
      || originals.some(action => !byKey.has(action.key) || !same(action, byKey.get(action.key)))) throw new Error("Original is not in the Driver decision");
  const { decision: _decision, ...metadata } = context;
  const compact = copyJson({ ...input, context: { ...metadata, decisionBinding: { encoding: "dispatch-originals-v1", frameID: context.frameID,
    digest: canonicalJsonDigest(decision), originals: originals.map(action => ({ key: action.key, digest: canonicalJsonDigest(action) })) } } });
  if (!compactValues || input.phase !== "admission") return compact;
  const out = compact as ObjectJson, ctx = out.context as ObjectJson, prepared = ctx.prepared as ObjectJson, original = ctx.original as ObjectJson;
  if (!record(prepared) || !record(original) || !same(prepared.input, out.value) || !same(prepared.config, out.config)) throw new Error("Invalid prepared value slots");
  const slots = ["prepared.input", "prepared.config"];
  prepared.input = null; prepared.config = null;
  if (same(original.input, out.value)) { original.input = null; slots.push("original.input"); }
  if (same(original.config, out.config)) { original.config = null; slots.push("original.config"); }
  ctx.valueSlots = slots; return out;
}

/** Preserve already-persisted legacy contexts on recovery/revalidation. */
export function dispatchPayloadMatches(expected: unknown, encoded: unknown): boolean {
  const actual = typeof encoded === "string" ? parseJson(encoded) : encoded;
  if (record(actual) && record(actual.context) && actual.context.policySource !== undefined) expected = compactPolicyPayload(expected, actual.context.policySource);
  const compact = record(actual) && record(actual.context) && Object.hasOwn(actual.context, "decisionBinding");
  return same(compact ? compactDispatchPayload(expected, true, record(actual) && record(actual.context) && Object.hasOwn(actual.context, "valueSlots")) : expected, actual);
}

export function dispatchOriginalMatches(context: ObjectJson, original: ActionRequest): boolean {
  if (!Object.hasOwn(context, "decisionBinding")) {
    const decision = validateDecision(context.decision);
    return decision.kind === "dispatch" && decision.actions.some(action => same(action, original));
  }
  const binding = context.decisionBinding;
  if (Object.hasOwn(context, "decision") || !record(binding) || Object.keys(binding).sort().join(",") !== "digest,encoding,frameID,originals"
      || binding.encoding !== "dispatch-originals-v1" || binding.frameID !== context.frameID
      || typeof binding.digest !== "string" || !/^[a-f0-9]{64}$/.test(binding.digest) || !Array.isArray(binding.originals)) return false;
  const keys = new Set<string>(); let expected: string | undefined;
  for (const entry of binding.originals) {
    if (!record(entry) || Object.keys(entry).sort().join(",") !== "digest,key" || typeof entry.key !== "string" || !entry.key
        || keys.has(entry.key) || typeof entry.digest !== "string" || !/^[a-f0-9]{64}$/.test(entry.digest)) return false;
    keys.add(entry.key); if (entry.key === original.key) expected = entry.digest;
  }
  return expected === canonicalJsonDigest(original);
}

/** Private aliases only reference the payload's own validated input/config.
 * The authenticated assignment still owns the complete semantic request. */
export function expandDispatchPayload(input: unknown): Json {
  const out = copyJson(input);
  if (!record(out) || !record(out.context) || !Object.hasOwn(out.context, "valueSlots")) return out;
  const ctx = out.context, slots = ctx.valueSlots;
  if (!Array.isArray(slots) || new Set(slots).size !== slots.length) throw new Error("Invalid validation aliases");
  if (out.phase === "admission") {
    if (!record(ctx.decisionBinding) || ctx.decisionBinding.encoding !== "dispatch-originals-v1"
        || !slots.includes("prepared.input") || !slots.includes("prepared.config")) throw new Error("Invalid admission value aliases");
  } else if (out.phase !== "report-output" || slots.length !== 1 || slots[0] !== "prepared.config") throw new Error("Invalid output value aliases");
  for (const slot of slots) {
    if (typeof slot !== "string" || !["prepared.input", "prepared.config", "original.input", "original.config"].includes(slot)) throw new Error("Invalid admission slot");
    const [owner, field] = slot.split(".") as [string, string], target = ctx[owner];
    if (!record(target) || target[field] !== null) throw new Error("Invalid admission slot content");
    target[field] = copyJson(field === "input" ? out.value : out.config);
  }
  delete ctx.valueSlots; return out;
}

export function compactReportPayload(input: unknown, existing?: unknown): Json {
  const previousValue = typeof existing === "string" ? parseJson(existing) : existing;
  const source = record(previousValue) && record(previousValue.context) ? previousValue.context.policySource : undefined;
  const out = compactPolicyPayload(input, source);
  if (existing !== undefined) {
    const previous = typeof existing === "string" ? parseJson(existing) : existing;
    if (!record(previous) || !record(previous.context) || !Object.hasOwn(previous.context, "valueSlots")) return out;
  }
  if (!record(out) || out.phase !== "report-output" || !record(out.context) || !record(out.context.prepared)) return out;
  let bytes = 0; for (const part of canonicalJsonChunks(out)) { bytes += part.length; if (bytes > 4096) break; }
  if (bytes <= 4096 && existing === undefined) return out;
  if (!same(out.config, out.context.prepared.config)) throw new Error("Invalid output configuration binding");
  out.context.prepared.config = null; out.context.valueSlots = ["prepared.config"]; return out;
}
