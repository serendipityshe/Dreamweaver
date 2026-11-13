import { DatabaseSync } from "node:sqlite";
import { resolve } from "node:path";
import { defineCapability } from "@dreamweaver/harness/agent";
import { canonical, digest, fail, json } from "../../agent/src/contract/common.js";
import type { Json } from "../../harness/src/index.js";

// Example-private storage, never the Harness database. Keep until every Run
// using this installation and every related Action reconciliation has finished.
export function journal(path: string) {
  const file = resolve(path);
  const setup = (db: DatabaseSync) => db.exec("CREATE TABLE IF NOT EXISTS memories(scope TEXT NOT NULL, action TEXT NOT NULL, revision INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL, sha256 TEXT NOT NULL, UNIQUE(scope,action))");
  return defineCapability({
    descriptor: {
      ref: { id: "example.memory.journal", version: "1" }, contract: { id: "example.memory.entries", version: "1" },
      configSchema: { type: "object", required: ["storage", "mode"], properties: { storage: { const: file }, mode: { enum: ["all", "latest"] } }, additionalProperties: false },
      inputSchema: { oneOf: [
        { type: "object", properties: { op: { const: "read" } }, required: ["op"], additionalProperties: false },
        { type: "object", properties: { op: { const: "append" }, value: {} }, required: ["op", "value"], additionalProperties: false },
        { type: "object", properties: { op: { const: "resolve" }, revision: { type: "integer", minimum: 1 }, sha256: { type: "string" } }, required: ["op", "revision", "sha256"], additionalProperties: false },
      ] }, outputSchema: { type: "object" }, effect: { kind: "idempotent", guarantee: "Unique (trusted scope, actionID) durable receipt; writes reconcile by the original key" }, scope: "required",
    },
    dependencies: { create: () => new DatabaseSync(file), dispose: db => db.close() },
    implementation: {
      async execute(input, ctx) {
        setup(ctx.deps); const scope = ctx.scope?.namespace ?? fail("Trusted scope is required", "FORBIDDEN");
        const req = input as { op: string; value?: Json; revision?: number; sha256?: string }, config = ctx.config as { mode: string };
        let value: Json;
        if (req.op === "append") {
          const body = canonical(req.value), sha256 = digest(req.value);
          ctx.deps.prepare("INSERT INTO memories(scope,action,body,sha256) VALUES(?,?,?,?) ON CONFLICT(scope,action) DO NOTHING").run(scope, ctx.actionID, body, sha256);
          const row = ctx.deps.prepare("SELECT revision,sha256 FROM memories WHERE scope=? AND action=?").get(scope, ctx.actionID)!;
          if (row.sha256 !== sha256) fail("Idempotency content mismatch"); value = json(row);
        } else if (req.op === "resolve") {
          const row = ctx.deps.prepare("SELECT revision,body,sha256 FROM memories WHERE scope=? AND revision=?").get(scope, req.revision!);
          if (!row || row.sha256 !== req.sha256 || digest(JSON.parse(row.body as string)) !== req.sha256) fail("Pinned memory is missing or corrupt");
          value = { revision: row.revision as number, value: json(JSON.parse(row.body as string)), sha256: row.sha256 as string };
        } else {
          const rows = ctx.deps.prepare("SELECT revision,body,sha256 FROM memories WHERE scope=? ORDER BY revision DESC LIMIT ?").all(scope, config.mode === "latest" ? 1 : 16).reverse();
          value = { entries: rows.map(row => ({ revision: row.revision as number, value: json(JSON.parse(row.body as string)), sha256: row.sha256 as string })) };
        }
        return { payload: { kind: "succeeded", value }, evidence: { scope, mode: config.mode }, measurements: [] };
      },
      async reconcile(input, ctx): Promise<import("../../harness/src/index.js").Report<import("../../harness/src/index.js").Reconciliation>> {
        setup(ctx.deps); const scope = ctx.scope?.namespace ?? fail("Trusted scope is required", "FORBIDDEN");
        if ((input as { op: string }).op !== "append") return { payload: { kind: "retryable" }, evidence: { readOnly: true, scope }, measurements: [] };
        const row = ctx.deps.prepare("SELECT revision,sha256 FROM memories WHERE scope=? AND action=?").get(scope, ctx.actionID);
        if (row) return { payload: { kind: "settled", result: { kind: "succeeded", value: json(row) } }, evidence: { actionID: ctx.actionID, scope }, measurements: [] };
        return { payload: { kind: "retryable" }, evidence: { scope, uniqueKey: ctx.actionID, guarantee: "Any late insert conflicts with the same durable unique key" }, measurements: [] };
      },
    },
  });
}
