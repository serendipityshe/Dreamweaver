import type { CapacityDefinition, DependencyBinding, Json, Ref } from "../../harness/src/index.js";
import type { ModelRequest, ModelResult, ModelPart, ModelUsage } from "./contract/types.js";
import type { ModelSelection, ProviderConfiguration } from "../../agent/src/contract/types.js";
import { canonical, copy, digest, equal, fail, fields, freeze, generated, json, positive, string } from "../../agent/src/contract/common.js";
import { request, result } from "./contract/validation.js";

const adapters = { "openai-responses": { id: "dw.model.openai-responses", version: "1" }, "anthropic-messages": { id: "dw.model.anthropic-messages", version: "1" } };
export interface ModelBinding {
  provider: string; revision: string; protocol: keyof typeof adapters; adapter: Ref;
  baseURL: string; accountIdentity: string; name: string;
  features: ProviderConfiguration["models"][string]["features"];
  parameters: NonNullable<ModelSelection["parameters"]>; providerOptions: Record<string, Json>;
}
type SDK = { send(body: Record<string, unknown>, signal: AbortSignal): Promise<any> };
const token = (value: unknown): number | null => typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
function parameters(value: unknown) {
  fields(value, ["temperature", "maxOutputTokens", "stop"]);
  if (value.temperature !== undefined && (typeof value.temperature !== "number" || !Number.isFinite(value.temperature) || value.temperature < 0 || value.temperature > 2)) fail("Invalid temperature");
  if (value.maxOutputTokens !== undefined) positive(value.maxOutputTokens);
  if (value.stop !== undefined && (!Array.isArray(value.stop) || !value.stop.length || value.stop.some((s: unknown) => typeof s !== "string" || !s.length))) fail("Invalid stop sequences");
}
export function resolveModel(selection: ModelSelection, providers: Record<string, ProviderConfiguration>) {
  const p = Object.hasOwn(providers, selection.provider) ? providers[selection.provider] : undefined; if (!p) return fail("Provider unavailable", "VERSION_UNAVAILABLE");
  fields(p, ["revision", "protocol", "baseURL", "accountIdentity", "apiKey", "models", "allowInsecureHTTP"], ["revision", "protocol", "accountIdentity", "apiKey", "models"]);
  if (Object.hasOwn(p, "allowInsecureHTTP") && typeof p.allowInsecureHTTP !== "boolean") fail("Invalid insecure HTTP option");
  string(p.revision); string(p.accountIdentity);
  if (!Object.hasOwn(adapters, p.protocol)) fail("Protocol not implemented", "VERSION_UNAVAILABLE");
  if (typeof p.apiKey !== "function" && typeof p.apiKey !== "string") fail("Invalid credential resolver");
  const model = Object.hasOwn(p.models, selection.name) ? p.models[selection.name]! : fail("Model unavailable", "VERSION_UNAVAILABLE");
  fields(model, ["features", "parameters", "providerOptions"], ["features"]);
  fields(model.features, ["text", "tools", "image", "jsonSchema", "streaming"], ["text", "tools", "image", "jsonSchema", "streaming"]);
  for (const [name, support] of Object.entries(model.features)) {
    if (!["supported", "unsupported", "unknown"].includes(support)) fail("Invalid feature declaration");
    if (!["text", "tools"].includes(name) && support === "supported") fail(`Adapter does not implement ${name}`);
  }
  if (model.features.text !== "supported") fail("Text support must be explicit");
  parameters(model.parameters ?? {}); parameters(selection.parameters ?? {});
  const effective = { ...(p.protocol === "anthropic-messages" ? { maxOutputTokens: 1024 } : {}), ...model.parameters, ...selection.parameters };
  if (p.protocol === "openai-responses" && effective.stop) fail("Responses stop sequences are not supported");
  const providerOptions = { ...copy(model.providerOptions ?? {}), ...copy(selection.providerOptions ?? {}) };
  fields(providerOptions, p.protocol === "openai-responses" ? ["reasoning"] : ["thinking"]);
  if (providerOptions.reasoning !== undefined) {
    fields(providerOptions.reasoning, ["effort"], ["effort"]);
    if (!["none", "minimal", "low", "medium", "high", "xhigh"].includes((providerOptions.reasoning as Record<string, any>).effort)) fail("Unsupported reasoning option");
  }
  if (providerOptions.thinking !== undefined) {
    const thinking = providerOptions.thinking as Record<string, any>;
    fields(thinking, thinking?.type === "disabled" ? ["type"] : ["type", "budget_tokens"], thinking?.type === "disabled" ? ["type"] : ["type", "budget_tokens"]);
    if (thinking.type === "enabled") positive(thinking.budget_tokens); else if (thinking.type !== "disabled") fail("Unsupported thinking mode");
  }
  const baseURL = p.baseURL ?? (p.protocol === "openai-responses" ? "https://api.openai.com/v1" : "https://api.anthropic.com");
  const url = new URL(baseURL); if (!["https:", "http:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) fail("Invalid provider URL");
  if (url.protocol === "http:" && p.allowInsecureHTTP !== true) fail("Insecure provider URL requires allowInsecureHTTP");
  const binding: ModelBinding = freeze(copy({ provider: selection.provider, revision: p.revision, protocol: p.protocol as ModelBinding["protocol"], adapter: adapters[p.protocol as keyof typeof adapters], baseURL: url.toString().replace(/\/$/, ""), accountIdentity: p.accountIdentity, name: selection.name, features: model.features, parameters: effective, providerOptions }));
  const bindingDigest = digest(binding), target = generated("model", binding), secret = p.apiKey;
  const validate = (input: Json) => { request(input); validateBinding(binding, bindingDigest, input); };
  const definition: CapacityDefinition<SDK> = {
    ref: target, validateInput: validate, validateOutput: result,
    async execute(input, ctx): Promise<import("../../harness/src/index.js").Report<import("../../harness/src/index.js").CapacityResult>> {
      validate(input); const req = input as unknown as ModelRequest;
      let raw: any;
      try { raw = await ctx.deps.send(encode(binding, req), ctx.signal); }
      catch (error) {
        // Only explicit request/auth/rate-limit rejections are determinate.
        const status = (error as { status?: number })?.status;
        if (status && [400, 401, 403, 404, 422, 429].includes(status)) return { payload: { kind: "failed", error: { code: status === 401 || status === 403 ? "MODEL_AUTH" : status === 429 ? "MODEL_RATE_LIMIT" : "MODEL_PROVIDER_REJECTED", message: "Provider rejected the request", details: { status } } }, evidence: { status }, measurements: [] };
        throw new Error("Model request outcome is unknown"); // no secret-bearing SDK error escapes
      }
      const output = decode(binding, bindingDigest, raw, req);
      result(output); ctx.reportProgress(json({ type: "completed", result: output }));
      return { payload: { kind: "succeeded", value: json(output) }, evidence: { adapter: json(binding.adapter), bindingDigest, responseID: output.responseID }, measurements: [] };
    },
    async reconcile() { return { payload: { kind: "unresolved", reason: { code: "MODEL_OUTCOME_UNKNOWN", message: "No reliable result lookup is configured; explicit review is required", details: { bindingDigest } } }, evidence: { adapter: json(binding.adapter), bindingDigest, querySupported: false }, measurements: [] }; },
  };
  const dependencies: DependencyBinding<SDK> = { async create() {
    let apiKey: string;
    try { apiKey = typeof secret === "function" ? await secret() : secret; string(apiKey); } catch { throw new Error("Model credentials unavailable before dispatch"); }
    // Dynamic imports: importing the caller entry never loads a provider SDK.
    // redirect:error also prevents an HTTP redirect from replaying a paid POST.
    if (binding.protocol === "openai-responses") {
      const { default: OpenAI } = await import("openai");
      const sdk = new OpenAI({ apiKey, baseURL: binding.baseURL, maxRetries: 0, fetchOptions: { redirect: "error" } });
      return { send: (body, signal) => sdk.responses.create(body as any, { signal, maxRetries: 0 }) };
    }
    const { default: Anthropic } = await import("@anthropic-ai/sdk");
    const sdk = new Anthropic({ apiKey, baseURL: binding.baseURL, maxRetries: 0, fetchOptions: { redirect: "error" } });
    return { send: (body, signal) => sdk.messages.create(body as any, { signal, maxRetries: 0 }) };
  } };
  return { target, binding, bindingDigest, definition, dependencies };
}
function providerParts(req: ModelRequest) { return req.messages.flatMap(m => m.content).filter(p => p.type === "provider"); }
export function validateModelTools(tools: { inputSchema: Record<string, Json> }[]) {
  // Both implemented function-tool protocols require an object argument root.
  // Never erase a root constraint to make a provider accept the declaration.
  for (const tool of tools) if (tool.inputSchema.type !== "object") fail("Model tools require an explicit object-root inputSchema");
}
export function validateBinding(binding: ModelBinding, bindingDigest: string, req: ModelRequest) {
  validateModelTools(req.tools);
  if (req.tools.length && binding.features.tools !== "supported") fail("Tools are not supported");
  if (req.output.kind !== "text" || req.messages.some(m => m.content.some(p => p.type === "image"))) fail("Requested model feature is not implemented");
  for (const p of [...providerParts(req), ...(req.providerState ? [req.providerState] : [])]) if (!equal(p.adapter, binding.adapter) || p.bindingDigest !== bindingDigest) fail("Provider data belongs to another binding");
  for (const p of providerParts(req)) {
    const v = p.value as Record<string, Json>;
    if (!v || typeof v !== "object" || ![...(binding.protocol === "openai-responses" ? ["reasoning"] : ["thinking", "redacted_thinking"])].includes(v.type as string)) fail("Unsupported provider block");
  }
  if (req.providerState) {
    fields(req.providerState.value, ["blocks"], ["blocks"]);
    if (!equal((req.providerState.value as Record<string, Json>).blocks, providerParts(req).map(p => digest(p.value)))) fail("Provider continuation blocks were removed or reordered");
  }
}
export function encode(binding: ModelBinding, req: ModelRequest): Record<string, unknown> {
  const parameters = binding.parameters;
  if (binding.protocol === "openai-responses") {
    const input: unknown[] = [];
    for (const m of req.messages) for (const p of m.content) {
      if (p.type === "text") input.push({ role: m.role, content: [{ type: m.role === "assistant" ? "output_text" : "input_text", text: p.text }] });
      else if (p.type === "tool-call") input.push({ type: "function_call", call_id: p.id, name: p.name, arguments: Object.hasOwn(p, "argumentsText") ? p.argumentsText : canonical(p.arguments) });
      else if (p.type === "tool-result") input.push({ type: "function_call_output", call_id: p.callID, output: canonical({ value: p.value, isError: p.isError }) });
      else if (p.type === "provider") input.push(copy(p.value));
      else fail("Unsupported input part");
    }
    return { model: binding.name, instructions: req.instructions, input, tools: req.tools.map(t => ({ type: "function", name: t.name, description: t.description, parameters: t.inputSchema, strict: false })), store: false, include: ["reasoning.encrypted_content"], ...(parameters.temperature !== undefined ? { temperature: parameters.temperature } : {}), ...(parameters.maxOutputTokens ? { max_output_tokens: parameters.maxOutputTokens } : {}), ...binding.providerOptions };
  }
  const messages: { role: string; content: unknown[] }[] = [];
  for (const m of req.messages) {
    const role = m.role === "assistant" ? "assistant" : "user";
    const content = m.content.map(p => {
      if (p.type === "text") return { type: "text", text: p.text };
      if (p.type === "tool-call") return { type: "tool_use", id: p.id, name: p.name, input: p.arguments };
      if (p.type === "tool-result") return { type: "tool_result", tool_use_id: p.callID, content: canonical(p.value), is_error: p.isError };
      if (p.type === "provider") return copy(p.value);
      return fail("Unsupported input part");
    });
    if (messages.at(-1)?.role === role) messages.at(-1)!.content.push(...content); else messages.push({ role, content });
  }
  return { model: binding.name, system: req.instructions, messages, tools: req.tools.map(t => ({ name: t.name, description: t.description, input_schema: t.inputSchema })), max_tokens: parameters.maxOutputTokens, ...(parameters.temperature !== undefined ? { temperature: parameters.temperature } : {}), ...(parameters.stop ? { stop_sequences: parameters.stop } : {}), ...binding.providerOptions };
}
export function decode(binding: ModelBinding, bindingDigest: string, raw: any, req: ModelRequest): ModelResult {
  const content: ModelPart[] = []; let finish: ModelResult["finish"]; let usage: ModelUsage;
  const opaque = (value: unknown): ModelPart => ({ type: "provider", adapter: copy(binding.adapter), bindingDigest, value: json(value) });
  if (binding.protocol === "openai-responses") {
    if (!Array.isArray(raw.output)) fail("Invalid Responses output");
    let refused = false, incomplete = false;
    for (const item of raw.output) {
      if (item.type === "message") {
        if (item.role !== "assistant" || !Array.isArray(item.content)) fail("Invalid response message");
        for (const p of item.content) { if (p.type === "output_text") content.push({ type: "text", text: p.text }); else if (p.type === "refusal") refused = true; else fail("Unsupported response content"); }
      } else if (item.type === "function_call") {
        try {
          if (typeof item.arguments !== "string") throw new Error("arguments");
          const text = item.arguments;
          content.push({ type: "tool-call", id: item.call_id, name: item.name, arguments: json(JSON.parse(text)), argumentsText: text });
        } catch { incomplete = true; }
      } else if (item.type === "reasoning") content.push(opaque(item)); else fail("Unsupported Responses item");
      if (item.status === "incomplete") incomplete = true;
    }
    if (!["completed", "incomplete"].includes(raw.status)) fail("Unknown Responses status");
    finish = raw.status === "incomplete" || incomplete ? "incomplete" : refused ? "refusal" : content.some(p => p.type === "tool-call") ? "tool-calls" : "stop";
    usage = { inputTokens: token(raw.usage?.input_tokens), outputTokens: token(raw.usage?.output_tokens), totalTokens: token(raw.usage?.total_tokens), cachedInputTokens: token(raw.usage?.input_tokens_details?.cached_tokens) };
  } else {
    if (!Array.isArray(raw.content) || raw.role !== "assistant") fail("Invalid Messages response");
    for (const p of raw.content) {
      if (p.type === "text") content.push({ type: "text", text: p.text });
      else if (p.type === "tool_use") content.push({ type: "tool-call", id: p.id, name: p.name, arguments: json(p.input) });
      else if (["thinking", "redacted_thinking"].includes(p.type)) content.push(opaque(p)); else fail("Unsupported Messages content");
    }
    const reasons: Record<string, ModelResult["finish"]> = { end_turn: "stop", stop_sequence: "stop", tool_use: "tool-calls", max_tokens: "incomplete", pause_turn: "incomplete", model_context_window_exceeded: "incomplete", refusal: "refusal" };
    finish = reasons[raw.stop_reason] ?? fail("Unknown Messages stop reason");
    const input = token(raw.usage?.input_tokens), output = token(raw.usage?.output_tokens), cached = token(raw.usage?.cache_read_input_tokens), created = token(raw.usage?.cache_creation_input_tokens);
    // Anthropic input_tokens excludes cached/write tokens; absent cache counts
    // remain unknown, rather than inventing a zero or understated total.
    const totalInput = input !== null && cached !== null && created !== null ? input + cached + created : null;
    usage = { inputTokens: totalInput, outputTokens: output, totalTokens: totalInput !== null && output !== null ? totalInput + output : null, cachedInputTokens: cached };
  }
  const blocks = [...providerParts(req), ...content.filter(p => p.type === "provider")].map(p => digest(p.value));
  return { content, finish, usage, responseID: raw.id ?? null, providerState: blocks.length ? { adapter: copy(binding.adapter), bindingDigest, value: { blocks } } : null };
}
