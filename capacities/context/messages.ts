import { defineCapability } from "@dreamweaver/harness/agent";
import type { ContextRequest, ContextResult } from "../../agent/src/contract/types.js";
import { messages } from "../model/contract/validation.js";
import { canonical, fail, json } from "../../agent/src/contract/common.js";

// Two policies share a domain contract. Recent retains whole user turns, never
// slices between a call and its results or silently drops opaque protocol data.
export function messageContext(mode: "all" | "recent") {
  return defineCapability({
    descriptor: {
      ref: { id: `example.context.${mode}`, version: "1" }, contract: { id: "dw.context.messages", version: "1" },
      configSchema: { type: "null" }, inputSchema: { type: "object", required: ["task", "messages", "maxOutputBytes"], properties: { task: { type: "string" }, messages: { type: "array" }, maxOutputBytes: { type: "integer", minimum: 1 } }, additionalProperties: false },
      outputSchema: { type: "object", required: ["messages", "sources"], properties: { messages: { type: "array" }, sources: { type: "array" } }, additionalProperties: false }, effect: { kind: "read" }, scope: "none",
    }, dependencies: { create: () => null },
    implementation: { async execute(input) {
      const req = input as unknown as ContextRequest; messages(req.messages);
      const last = req.messages.findLastIndex(m => m.role === "user");
      const start = mode === "recent" ? Math.max(0, last) : 0;
      if (req.messages.slice(0, start).some(m => m.content.some(p => p.type === "provider"))) fail("Recent context cannot discard required provider state");
      const output: ContextResult = { messages: req.messages.slice(start), sources: [] }; messages(output.messages);
      if (Buffer.byteLength(canonical(output.messages)) > req.maxOutputBytes) fail("Context exceeds requested size");
      return { payload: { kind: "succeeded", value: json(output) }, evidence: { mode }, measurements: [] };
    } },
  });
}
