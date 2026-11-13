import type { ModelMessage, ModelPart, ModelRequest, ModelResult } from "./types.js";
import { HarnessError } from "../../../harness/src/index.js";
import { canonical, fields, fail, json, ref, string } from "../../../agent/src/contract/common.js";
import { validator } from "../../../agent/src/contract/schema.js";

function part(p: ModelPart, role: ModelMessage["role"]) {
  if (!p || typeof p !== "object") fail("Invalid model part");
  switch (p.type) {
    case "text": fields(p, ["type", "text"], ["type", "text"]); if (typeof p.text !== "string") fail("Invalid text"); break;
    case "image": fields(p, ["type", "mediaType", "data"], ["type", "mediaType", "data"]); string(p.mediaType); string(p.data); if (role !== "user") fail("Invalid image position"); break;
    case "tool-call": {
      fields(p, ["type", "id", "name", "arguments", "argumentsText"], ["type", "id", "name", "arguments"]); string(p.id); string(p.name); json(p.arguments);
      if (Object.hasOwn(p, "argumentsText")) {
        if (typeof p.argumentsText !== "string") fail("Invalid tool arguments text");
        try { if (canonical(JSON.parse(p.argumentsText)) !== canonical(p.arguments)) fail("Tool arguments text does not match arguments"); }
        catch (error) { if (error instanceof HarnessError) throw error; fail("Invalid tool arguments text"); }
      }
      if (role !== "assistant") fail("Invalid call role"); break;
    }
    case "tool-result": fields(p, ["type", "callID", "value", "isError"], ["type", "callID", "value", "isError"]); string(p.callID); json(p.value); if (role !== "tool" || typeof p.isError !== "boolean") fail("Invalid result role"); break;
    case "provider": fields(p, ["type", "adapter", "bindingDigest", "value"], ["type", "adapter", "bindingDigest", "value"]); ref(p.adapter, false); if (role !== "assistant" || !/^[0-9a-f]{64}$/.test(p.bindingDigest)) fail("Invalid provider block"); json(p.value); break;
    default: fail("Unknown model part");
  }
}
export function messages(messages: ModelMessage[], requireResults = true): void {
  if (!Array.isArray(messages)) fail("Expected messages");
  const seen = new Set<string>(), pending = new Set<string>();
  for (const message of messages) {
    fields(message, ["role", "content"], ["role", "content"]);
    if (!["user", "assistant", "tool"].includes(message.role) || !Array.isArray(message.content)) fail("Invalid message");
    if (message.role !== "tool" && pending.size) fail("Missing tool results before next message");
    for (const p of message.content) {
      part(p, message.role);
      if (message.role === "tool" && p.type !== "tool-result") fail("Tool message must contain results");
      if (p.type === "tool-call") { if (seen.has(p.id)) fail("Duplicate call ID"); seen.add(p.id); pending.add(p.id); }
      if (p.type === "tool-result") { if (!pending.delete(p.callID)) fail("Unmatched or duplicate tool result"); }
    }
  }
  if (requireResults && pending.size) fail("Unresolved tool calls");
}
export function request(value: unknown): asserts value is ModelRequest {
  json(value); fields(value, ["instructions", "messages", "tools", "output", "providerState"], ["instructions", "messages", "tools", "output", "providerState"]);
  if (typeof value.instructions !== "string" || !Array.isArray(value.tools)) fail("Invalid model request");
  messages(value.messages);
  const names = new Set<string>();
  for (const t of value.tools) {
    fields(t, ["name", "description", "inputSchema"], ["name", "description", "inputSchema"]);
    if (!/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(t.name) || names.has(t.name) || typeof t.description !== "string") fail("Invalid tool declaration");
    names.add(t.name); validator(t.inputSchema);
  }
  fields(value.output, value.output?.kind === "text" ? ["kind"] : ["kind", "schema"], ["kind"]);
  if (value.output.kind === "json-schema") validator(value.output.schema); else if (value.output.kind !== "text") fail("Unknown output mode");
  if (value.providerState !== null) { fields(value.providerState, ["adapter", "bindingDigest", "value"], ["adapter", "bindingDigest", "value"]); ref(value.providerState.adapter, false); if (!/^[0-9a-f]{64}$/.test(value.providerState.bindingDigest)) fail("Invalid provider state"); }
}
export function result(value: unknown): asserts value is ModelResult {
  json(value); fields(value, ["content", "finish", "usage", "providerState", "responseID"], ["content", "finish", "usage", "providerState", "responseID"]);
  if (!Array.isArray(value.content)) fail("Invalid model content");
  const ids = new Set<string>();
  for (const p of value.content) { part(p, "assistant"); if (p.type === "tool-call") { if (ids.has(p.id)) fail("Duplicate call ID"); ids.add(p.id); } }
  if (!["stop", "tool-calls", "refusal", "incomplete"].includes(value.finish) || value.finish === "stop" && ids.size || value.finish === "tool-calls" && !ids.size) fail("Conflicting model finish");
  fields(value.usage, ["inputTokens", "outputTokens", "totalTokens", "cachedInputTokens"], ["inputTokens", "outputTokens", "totalTokens", "cachedInputTokens"]);
  for (const n of Object.values(value.usage)) if (n !== null && (!Number.isSafeInteger(n) || (n as number) < 0)) fail("Invalid usage");
  if (value.responseID !== null) string(value.responseID);
  if (value.providerState !== null) { fields(value.providerState, ["adapter", "bindingDigest", "value"], ["adapter", "bindingDigest", "value"]); ref(value.providerState.adapter, false); if (!/^[0-9a-f]{64}$/.test(value.providerState.bindingDigest)) fail("Invalid provider state"); }
}
