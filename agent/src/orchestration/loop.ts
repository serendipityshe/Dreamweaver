import { HarnessError, type ActionRequest, type AgentDriver, type Decision, type Json } from "../../../harness/src/index.js";
import type { AgentDefinitionOptions, ContextResult, StrategyEnvironment } from "../contract/types.js";
import type { AgentAnswer, ModelMessage, ModelRequest, ModelResult, ModelUsage } from "../../../capacities/model/contract/types.js";
import { messages, result as validateResult } from "../../../capacities/model/contract/validation.js";
import { canonical, copy, fail, fields, json, ref } from "../contract/common.js";
import { validator } from "../contract/schema.js";
import { failed, failure } from "./initialize.js";

interface State {
  phase: "context" | "model" | "tool"; round: number; context: number; task: string;
  transcript: ModelMessage[]; current: ModelMessage[]; providerState: ModelRequest["providerState"];
  pending: { key: string; callID: string | null }[];
  usage: ModelUsage; missing: number;
}
export function toolLoop(options: AgentDefinitionOptions, env: StrategyEnvironment): AgentDriver {
  const limits = options.limits as Required<NonNullable<AgentDefinitionOptions["limits"]>>;
  const tools = options.tools ?? [], contexts = options.context ?? [];
  const schemas = new Map(tools.map(t => [t.name, validator(t.inputSchema)]));
  const within = (s: State) => [s.transcript, s.current].every(messages => Buffer.byteLength(canonical({ messages, providerState: s.providerState })) <= limits.maxTranscriptBytes);
  function continuation(input: Json): State {
    fields(input, ["phase", "round", "context", "task", "transcript", "current", "providerState", "pending", "usage", "missing"], ["phase", "round", "context", "task", "transcript", "current", "providerState", "pending", "usage", "missing"]);
    const value = input as Record<string, any>;
    if (!["context", "model", "tool"].includes(value.phase)) fail("Unknown loop phase");
    const natural = (n: unknown) => { if (!Number.isSafeInteger(n) || (n as number) < 0) fail("Invalid loop counter"); };
    natural(value.round); natural(value.context); natural(value.missing);
    if ((value.missing as number) > (value.round as number) || (value.context as number) > contexts.length || value.phase === "context" && (value.context as number) >= contexts.length) fail("Invalid loop position");
    if (typeof value.task !== "string" || !value.task.trim()) fail("Invalid loop task");
    if (!Array.isArray(value.transcript) || !Array.isArray(value.current) || !Array.isArray(value.pending)) fail("Invalid loop collections");
    messages(value.current as ModelMessage[]);
    messages(value.transcript as ModelMessage[], value.phase !== "tool");
    if (value.providerState !== null) {
      fields(value.providerState, ["adapter", "bindingDigest", "value"], ["adapter", "bindingDigest", "value"]);
      ref(value.providerState.adapter, false);
      if (!/^[0-9a-f]{64}$/.test(value.providerState.bindingDigest)) fail("Invalid provider state");
      json(value.providerState.value);
    }
    fields(value.usage, ["inputTokens", "outputTokens", "totalTokens", "cachedInputTokens"], ["inputTokens", "outputTokens", "totalTokens", "cachedInputTokens"]);
    for (const n of Object.values(value.usage)) if (n !== null && (!Number.isSafeInteger(n) || (n as number) < 0)) fail("Invalid usage");
    const keys = new Set<string>(), calls = new Set<string>();
    for (const item of value.pending) {
      fields(item, ["key", "callID"], ["key", "callID"]);
      if (typeof item.key !== "string" || !item.key || keys.has(item.key)) fail("Invalid pending key");
      keys.add(item.key);
      if (value.phase === "tool") { if (typeof item.callID !== "string" || !item.callID || calls.has(item.callID)) fail("Invalid pending call"); calls.add(item.callID); }
      else if (item.callID !== null) fail("Invalid pending call");
    }
    if (value.phase === "tool" ? !value.pending.length : value.pending.length !== 1) fail("Invalid pending set");
    return value as unknown as State;
  }
  function dispatch(s: State, actions: ActionRequest[]): Decision {
    if (!within(s)) return failure("AGENT_CONTEXT_LIMIT", "Transcript exceeded byte limit");
    return { kind: "dispatch", continuation: { entry: "dw.agent.loop/1", state: json(s) }, actions, wait: { all: actions.map(a => a.key) } };
  }
  function next(s: State): Decision {
    if (s.round >= limits.maxModelCalls) return failure("AGENT_MODEL_LIMIT", "Model action limit reached");
    if (s.context < contexts.length) {
      s.phase = "context"; const cap = env.capabilities[contexts[s.context]!]!, key = `context:${s.round}:${s.context}`;
      s.pending = [{ key, callID: null }];
      return dispatch(s, [{ key, kind: "capacity", target: cap.target, config: cap.config, input: json({ task: s.task, messages: s.current, maxOutputBytes: limits.maxTranscriptBytes }) }]);
    }
    s.phase = "model"; const key = `model:${s.round}`; s.pending = [{ key, callID: null }];
    return dispatch(s, [{ key, kind: "capacity", target: env.model!, config: null, input: json({ instructions: options.instructions, messages: s.current, tools: tools.map(t => ({ name: t.name, description: t.description, inputSchema: t.inputSchema })), output: { kind: "text" }, providerState: s.providerState }) }]);
  }
  return {
    ref: env.agent,
    validateInput(input) { if (typeof input !== "string" || !input.trim()) fail("Default Agent input must be nonempty text"); },
    validateOutput(output) {
      fields(output, ["text", "content", "usage"], ["text", "content", "usage"]);
      const answer = output as unknown as AgentAnswer;
      fields(answer.usage, ["known", "modelActions", "missingUsageActions"], ["known", "modelActions", "missingUsageActions"]);
      validateResult({ content: answer.content, finish: "stop", usage: answer.usage.known, providerState: null, responseID: null });
      if (answer.text !== answer.content.filter(p => p.type === "text").map(p => p.text).join("") || !Number.isSafeInteger(answer.usage.modelActions) || answer.usage.modelActions < 1 || !Number.isSafeInteger(answer.usage.missingUsageActions) || answer.usage.missingUsageActions < 0 || answer.usage.missingUsageActions > answer.usage.modelActions) fail("Invalid AgentAnswer");
    },
    advance(input) {
      if (input.wake.kind === "start") {
        const transcript: ModelMessage[] = [{ role: "user", content: [{ type: "text", text: input.run.input as string }] }];
        return next({ phase: "model", round: 0, context: 0, task: input.run.input as string, transcript, current: transcript, providerState: null, pending: [], usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0, cachedInputTokens: 0 }, missing: 0 });
      }
      let s: State;
      try {
        if (input.continuation.entry !== "dw.agent.loop/1") fail("Unknown loop continuation", "INVALID_DECISION");
        s = continuation(copy(input.continuation.state));
      } catch (error) {
        if (error instanceof HarnessError && error.info.code === "INVALID_DECISION") throw error;
        throw new HarnessError({ code: "INVALID_DECISION", message: "Invalid loop continuation", disposition: "rejected", details: null, retryAfterMs: null }, { cause: error });
      }
      if (input.wake.kind === "timeout") return failure("AGENT_WAIT_TIMEOUT", "Action wait timed out");
      const facts = s.pending.map(p => { const f = input.wake.kind === "results" && input.wake.facts.find(f => f.actionKey === p.key); return f || fail("Missing awaited fact", "INVALID_DECISION"); });
      for (const f of facts) {
        if (f.outcome.kind === "cancelled") return failure("AGENT_ACTION_CANCELLED", "An awaited Action was cancelled");
        if (f.outcome.kind === "failed" && s.phase !== "tool") return failed(f.outcome.error);
      }
      if (s.phase === "context") {
        const value = (facts[0]!.outcome as { kind: "succeeded"; value: Json }).value as unknown as ContextResult;
        messages(value.messages); if (!Array.isArray(value.sources)) fail("Invalid context result", "INVALID_DECISION");
        s.current = value.messages; s.context++; return next(s);
      }
      if (s.phase === "tool") {
        s.transcript.push({ role: "tool", content: facts.map((f, index) => ({ type: "tool-result", callID: s.pending[index]!.callID!, value: f.outcome.kind === "succeeded" ? f.outcome.value : json((f.outcome as { kind: "failed"; error: unknown }).error), isError: f.outcome.kind === "failed" })) });
        s.current = s.transcript; s.context = 0; return next(s);
      }
      const model = (facts[0]!.outcome as { kind: "succeeded"; value: Json }).value as unknown as ModelResult; validateResult(model);
      s.round++;
      for (const k of Object.keys(s.usage) as (keyof ModelUsage)[]) s.usage[k] = s.usage[k] === null || model.usage[k] === null ? null : s.usage[k]! + model.usage[k]!;
      if ([model.usage.inputTokens, model.usage.outputTokens, model.usage.totalTokens].includes(null)) s.missing++;
      if (model.finish === "refusal") return failure("MODEL_REFUSED", "Model refused the request");
      if (model.finish === "incomplete") return failure("MODEL_INCOMPLETE", "Model output is incomplete");
      s.transcript.push({ role: "assistant", content: model.content }); s.providerState = model.providerState;
      if (!within(s)) return failure("AGENT_CONTEXT_LIMIT", "Transcript exceeded byte limit");
      if (model.finish === "stop") return { kind: "complete", remainingActions: "cancel", outcome: { kind: "succeeded", value: json({ text: model.content.filter(p => p.type === "text").map(p => p.text).join(""), content: model.content, usage: { known: s.usage, modelActions: s.round, missingUsageActions: s.missing } } satisfies AgentAnswer) } };
      const calls = model.content.filter(p => p.type === "tool-call");
      try {
        if (calls.length > limits.maxToolCallsPerRound || new Set(calls.map(c => c.id)).size !== calls.length) fail("Invalid call batch");
        for (const c of calls) { const validate = schemas.get(c.name); if (!validate) fail("Unknown tool"); validate(c.arguments); }
      } catch { return failure("AGENT_INVALID_TOOL_CALL", "Tool batch failed preflight"); }
      s.phase = "tool"; s.pending = calls.map((c, i) => ({ key: `tool:${s.round - 1}:${i}`, callID: c.id }));
      return dispatch(s, calls.map((c, i) => ({ key: s.pending[i]!.key, kind: "capacity", target: env.tools[c.name]!.target, input: c.arguments, config: env.tools[c.name]!.config })));
    },
  };
}
