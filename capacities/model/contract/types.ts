import type { Json, Ref, Issue } from "../../../harness/src/index.js";
import type { JsonSchema } from "../../../agent/src/contract/schema.js";
export type Support = "supported" | "unsupported" | "unknown";
export interface ModelFeatures {
  text: Support; tools: Support; image: Support;
  jsonSchema: Support; streaming: Support;
}
export type ModelPart =
  | { type: "text"; text: string }
  | { type: "image"; mediaType: string; data: string } // 首版仅显式启用的 base64 小值
  | { type: "tool-call"; id: string; name: string; arguments: Json; argumentsText?: string }
  | { type: "tool-result"; callID: string; value: Json; isError: boolean }
  | { type: "provider"; adapter: Ref; bindingDigest: string; value: Json };
export interface ModelMessage {
  role: "user" | "assistant" | "tool";
  content: ModelPart[];
}
export interface ModelTool {
  name: string; description: string; inputSchema: JsonSchema;
}
export interface ModelRequest {
  instructions: string;
  messages: ModelMessage[];
  tools: ModelTool[];
  output: { kind: "text" } | { kind: "json-schema"; schema: JsonSchema };
  providerState: { adapter: Ref; bindingDigest: string; value: Json } | null;
}
export interface ModelUsage {
  inputTokens: number | null;
  outputTokens: number | null;
  totalTokens: number | null;
  cachedInputTokens: number | null;
}
export interface ModelResult {
  content: ModelPart[];
  finish: "stop" | "tool-calls" | "refusal" | "incomplete";
  usage: ModelUsage;
  providerState: ModelRequest["providerState"];
  responseID: string | null;
}
export type ModelEvent =
  | { type: "text-delta"; index: number; text: string }
  | { type: "tool-delta"; index: number; callID: string | null; argumentsText: string }
  | { type: "completed"; result: ModelResult }
  | { type: "error"; issue: Issue };
export interface AgentAnswer {
  text: string;
  content: ModelPart[];
  usage: {
    known: ModelUsage;
    modelActions: number;
    missingUsageActions: number;
  };
}
