import { defineAgent } from "@dreamweaver/harness/agent";

// A deterministic offline fixture, not a model. Real Agents normally use the default model strategy.
export const echoAgent = defineAgent({ ref: { id: "example.minimal", version: "1" }, strategy: {
  ref: { id: "example.echo-strategy", version: "1" },
  create(environment) {
    const validate = value => { if (typeof value !== "string") throw new Error("Expected a string"); };
    return { ref: environment.agent, validateInput: validate, validateOutput: validate,
      advance({ run }) { return { kind: "complete", outcome: { kind: "succeeded", value: run.input }, remainingActions: "cancel" }; } };
  },
} });
