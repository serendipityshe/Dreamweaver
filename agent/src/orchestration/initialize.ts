import type { AgentDriver, CapacityDefinition, Decision, Issue, Json, Ref } from "../../../harness/src/index.js";
import { copy, digest, equal, fail, generated, json } from "../contract/common.js";

export const initializationRef = generated("initialize", { sdk: "1", input: "sha256", output: "sha256" });
const validateDigest = (v: Json) => { if (typeof v !== "string" || !/^[0-9a-f]{64}$/.test(v)) fail("Invalid manifest digest"); };
export const initialization: CapacityDefinition<null> = {
  ref: initializationRef, validateInput: validateDigest, validateOutput: validateDigest,
  async execute(input) { return { payload: { kind: "succeeded", value: input }, evidence: { pure: true }, measurements: [] }; },
  async reconcile(input) { return { payload: { kind: "settled", result: { kind: "succeeded", value: input } }, evidence: { pure: true }, measurements: [] }; },
};
export const failure = (code: string, message: string, details: Json = null): Decision => failed({ code, message, details });
export const failed = (error: Issue): Decision => ({ kind: "complete", outcome: { kind: "failed", error }, remainingActions: "cancel" });
export function initialize(driver: AgentDriver, manifest: Json, allowed: { kind: string; target: Ref; config: Json }[]): AgentDriver {
  const manifestDigest = digest(manifest);
  return {
    ref: copy(driver.ref), validateInput: driver.validateInput.bind(driver), validateOutput: driver.validateOutput.bind(driver),
    async advance(input) {
      if (input.wake.kind === "start") return { kind: "dispatch", continuation: { entry: "dw.agent/1", state: json({ manifest, manifestDigest, initializing: true, continuation: input.continuation }) }, actions: [{ key: "dw:init:0", kind: "capacity", target: initializationRef, input: manifestDigest, config: null }], wait: { all: ["dw:init:0"] } };
      const state = input.continuation.state as any;
      if (input.continuation.entry !== "dw.agent/1" || !state || state.manifestDigest !== manifestDigest || digest(state.manifest) !== manifestDigest) fail("Agent manifest changed", "INVALID_DECISION");
      let wake = input.wake;
      if (state.initializing) {
        if (wake.kind === "timeout") return failure("AGENT_WAIT_TIMEOUT", "Initialization wait timed out");
        const fact = wake.facts.find(f => f.actionKey === "dw:init:0");
        if (!fact) fail("Missing initialization fact", "INVALID_DECISION");
        if (fact.outcome.kind === "failed") return failed(fact.outcome.error);
        if (fact.outcome.kind === "cancelled") return failure("AGENT_ACTION_CANCELLED", "Initialization cancelled");
        if (fact.outcome.value !== manifestDigest) fail("Initialization binding mismatch", "INVALID_DECISION");
      }
      const decision = await driver.advance({ ...input, continuation: copy(state.continuation), wake: state.initializing ? { kind: "start" } : wake });
      if (decision.kind === "complete") return decision;
      for (const action of decision.actions) {
        if (action.key.startsWith("dw:") || !allowed.some(a => a.kind === action.kind && equal(a.target, action.target) && equal(a.config, action.config))) fail("Strategy action is outside its declared bindings", "INVALID_DECISION");
      }
      return { ...decision, continuation: { entry: "dw.agent/1", state: json({ manifest, manifestDigest, initializing: false, continuation: decision.continuation }) } };
    },
  };
}
