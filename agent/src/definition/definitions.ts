import type { CapabilityInstallation, InstalledCapability, ToolDefinition, AgentDefinition, AgentDefinitionOptions } from "../contract/types.js";
import { copy, fields, freeze, positive, ref, string, fail, json } from "../contract/common.js";
import { validator } from "../contract/schema.js";

export const agents = new WeakMap<AgentDefinition, AgentDefinitionOptions>();
export const capabilities = new WeakMap<InstalledCapability, CapabilityInstallation<any>>();
const tools = new WeakSet<ToolDefinition>();
function effect(value: ToolDefinition["effect"], reconcile: unknown) {
  fields(value, value.kind === "idempotent" ? ["kind", "guarantee"] : ["kind"], ["kind"]);
  if (!["read", "idempotent", "external"].includes(value.kind)) fail("Unknown effect");
  if (value.kind === "idempotent") { string(value.guarantee); if (typeof reconcile !== "function") fail("Idempotent effects require reconciliation"); }
  if (reconcile !== undefined && typeof reconcile !== "function") fail("Invalid reconcile");
}
export function toolDescriptor(t: ToolDefinition) { const { execute, reconcile, ...data } = t; return copy(data); }
export function defineTool(definition: ToolDefinition): ToolDefinition {
  fields(definition, ["ref", "name", "description", "inputSchema", "outputSchema", "effect", "execute", "reconcile"], ["ref", "name", "description", "inputSchema", "outputSchema", "effect", "execute"]);
  ref(definition.ref);
  if (!/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(definition.name) || typeof definition.description !== "string" || typeof definition.execute !== "function") fail("Invalid tool declaration");
  effect(definition.effect, definition.reconcile); validator(definition.inputSchema); validator(definition.outputSchema);
  const result = freeze({ ...toolDescriptor(definition), execute: definition.execute, ...(definition.reconcile ? { reconcile: definition.reconcile } : {}) });
  tools.add(result); return result;
}
export function defineCapability<TDeps>(installation: CapabilityInstallation<TDeps>): InstalledCapability {
  fields(installation, ["descriptor", "implementation", "dependencies"], ["descriptor", "implementation", "dependencies"]);
  const d = copy(installation.descriptor);
  fields(d, ["ref", "contract", "configSchema", "inputSchema", "outputSchema", "effect", "scope"], ["ref", "contract", "configSchema", "inputSchema", "outputSchema", "effect", "scope"]);
  ref(d.ref); ref(d.contract); validator(d.configSchema); validator(d.inputSchema); validator(d.outputSchema);
  fields(installation.implementation, ["execute", "reconcile"], ["execute"]);
  fields(installation.dependencies, ["create", "dispose"], ["create"]);
  if (typeof installation.implementation.execute !== "function" || typeof installation.dependencies.create !== "function" || installation.dependencies.dispose !== undefined && typeof installation.dependencies.dispose !== "function") fail("Invalid capability implementation");
  effect(d.effect, installation.implementation.reconcile);
  if (!["none", "required"].includes(d.scope)) fail("Invalid scope requirement");
  const result = freeze({ descriptor: d });
  capabilities.set(result, { descriptor: d, implementation: { ...installation.implementation }, dependencies: { ...installation.dependencies } });
  return result;
}
export function defineAgent(options: AgentDefinitionOptions): AgentDefinition {
  fields(options, ["ref", "instructions", "model", "tools", "capabilities", "context", "delegates", "strategy", "limits"], ["ref"]); ref(options.ref);
  if (options.instructions !== undefined && typeof options.instructions !== "string") fail("Invalid instructions");
  if (!options.strategy && !options.model) fail("Default strategy requires a model");
  if (options.strategy) { fields(options.strategy, ["ref", "create"], ["ref", "create"]); ref(options.strategy.ref); if (typeof options.strategy.create !== "function") fail("Invalid strategy factory"); }
  if (options.model) {
    fields(options.model, ["provider", "name", "parameters", "providerOptions"], ["provider", "name"]); string(options.model.provider); string(options.model.name); json(options.model);
  }
  if (options.tools !== undefined && !Array.isArray(options.tools) || options.context !== undefined && !Array.isArray(options.context)) fail("Expected list");
  const ts = (options.tools ?? []).map(t => tools.has(t) ? t : defineTool(t));
  if (new Set(ts.map(t => t.name)).size !== ts.length) fail("Duplicate tool name", "DUPLICATE_DEFINITION");
  const bindings = copy(options.capabilities ?? {});
  fields(bindings, Object.keys(bindings));
  for (const [alias, b] of Object.entries(bindings)) {
    string(alias); fields(b, ["ref", "contract", "config", "scope"], ["ref", "contract"]); ref(b.ref); ref(b.contract);
    if (b.scope) { fields(b.scope, b.scope.kind === "grant" ? ["kind", "name"] : ["kind"], ["kind"]); if (b.scope.kind === "grant") string(b.scope.name); else if (b.scope.kind !== "run") fail("Invalid scope"); }
  }
  const context = copy(options.context ?? []);
  if (new Set(context).size !== context.length) fail("Duplicate context alias");
  for (const name of context) if (!Object.hasOwn(bindings, name) || bindings[name]!.contract.id !== "dw.context.messages" || bindings[name]!.contract.version !== "1") fail("Unknown or incompatible context contract");
  const delegates = copy(options.delegates ?? {}); fields(delegates, Object.keys(delegates)); Object.values(delegates).forEach(r => ref(r));
  fields(options.limits ?? {}, ["maxModelCalls", "maxToolCallsPerRound", "maxTranscriptBytes"]);
  const limits = { maxModelCalls: 16, maxToolCallsPerRound: 8, maxTranscriptBytes: 65536, ...options.limits }; Object.values(limits).forEach(positive);
  const normalized: AgentDefinitionOptions = { ref: copy(options.ref), instructions: options.instructions ?? "", ...(options.model ? { model: copy(options.model) } : {}), tools: ts, capabilities: bindings, context, delegates, limits, ...(options.strategy ? { strategy: { ...options.strategy, ref: copy(options.strategy.ref) } } : {}) };
  const descriptor = json({ ...normalized, tools: ts.map(toolDescriptor), strategy: options.strategy?.ref ?? { id: "dw.agent.tool-loop", version: "1" } });
  const result = freeze({ ref: copy(options.ref), descriptor }); agents.set(result, freeze(normalized)); return result;
}
