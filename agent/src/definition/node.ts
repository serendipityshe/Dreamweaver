import type { CapacityDefinition, DependencyBinding, HarnessNode, HarnessSession, HostPolicy, Json, Ref } from "../../../harness/src/index.js";
import type { AgentDefinition, AgentDeploymentPlan, AgentNode, AgentNodeOptions, CapabilityExecutionContext, StrategyEnvironment } from "../contract/types.js";
import { AgentExecutionError } from "../contract/errors.js";
import { canonical, copy, digest, equal, fail, fields, freeze, generated, json, ref, string } from "../contract/common.js";
import { validator } from "../contract/schema.js";
import { contextResult } from "../contract/context.js";
import { agents, capabilities, toolDescriptor } from "./definitions.js";
import { initialization, initializationRef, initialize } from "../orchestration/initialize.js";
import { toolLoop } from "../orchestration/loop.js";
import { resolveModel, validateModelTools } from "../../../capacities/model/managed.js";

type Capacity = { definition: CapacityDefinition<any>; dependencies: DependencyBinding<any>; config: Json; owner: string | null; identity: object };
export function createAgentNode(session: HarnessSession, options: AgentNodeOptions): AgentNode {
  fields(options, ["node", "providers", "capabilities", "scopeOwner", "grants"], ["node"]);
  fields(options.node, ["nodeID", "concurrency", "reportSpool", "policies", "onProgress", "onMetric", "onError"], ["nodeID", "concurrency"]);
  // Separate callback/secret references from copied behavior data.
  const { onProgress, onMetric, onError, ...data } = options.node;
  const nodeOptions = freeze(copy(data)), grants = freeze(copy(options.grants ?? {}));
  for (const grant of Object.values(grants)) { fields(grant, ["namespace"], ["namespace"]); string(grant.namespace); }
  const owner = options.scopeOwner ?? session.binding.principalID; string(owner);
  const domainID = session.binding.domainID;
  const providers = Object.fromEntries(Object.entries(options.providers ?? {}).map(([id, p]) => {
    fields(p, ["revision", "protocol", "baseURL", "accountIdentity", "apiKey", "models", "allowInsecureHTTP"], ["revision", "protocol", "accountIdentity", "apiKey", "models"]);
    if (Object.hasOwn(p, "allowInsecureHTTP") && typeof p.allowInsecureHTTP !== "boolean") fail("Invalid insecure HTTP option");
    const { apiKey, ...data } = p; return [id, freeze({ ...copy(data), apiKey })];
  }));
  const installed = new Map<string, NonNullable<ReturnType<typeof capabilities.get>>>();
  for (const c of options.capabilities ?? []) {
    const installation = capabilities.get(c) ?? fail("Capability must be created by defineCapability");
    const key = canonical(installation.descriptor.ref);
    if (installed.has(key)) fail("Duplicate capability installation", "DUPLICATE_DEFINITION"); installed.set(key, installation);
  }
  const definitions: AgentDefinition[] = [], policies: HostPolicy[] = [];
  let state = "collecting", plan: AgentDeploymentPlan, compiled: { drivers: ReturnType<typeof initialize>[]; capacities: Capacity[]; mandatoryPolicies: HostPolicy[]; selectedPolicies: { ref: Ref; config: Json }[] };
  let underlying: HarnessNode | undefined;
  function collecting() { if (state !== "collecting") fail("Node registration is frozen", "NODE_STATE_INVALID"); }
  function prepare(): AgentDeploymentPlan {
    if (state === "prepared") return copy(plan);
    collecting();
    try {
      for (const selected of nodeOptions.policies ?? []) if (!policies.some(p => equal(p.ref, selected.ref))) fail("Selected policy is not registered", "VERSION_UNAVAILABLE");
      const cs = new Map<string, Capacity>();
      function add(cap: Capacity) {
        const key = canonical(cap.definition.ref), prior = cs.get(key);
        if (prior && prior.identity !== cap.identity) fail("Conflicting local implementations for the same execution Ref", "DUPLICATE_DEFINITION");
        if (!prior) cs.set(key, cap);
      }
      add({ definition: initialization, dependencies: { create: () => null }, config: null, owner: null, identity: initialization });
      const records = definitions.map(definition => {
        const d = agents.get(definition)!;
        const environment: StrategyEnvironment = { agent: d.ref, manifest: null, model: null, capabilities: Object.create(null), tools: Object.create(null), delegates: copy(d.delegates ?? {}) };
        const bindings: Record<string, Json> = Object.create(null);
        const used: { kind: "capacity" | "agent"; target: Ref; config: Json }[] = [{ kind: "capacity", target: initializationRef, config: null }];
        if (d.model) {
          validateModelTools(d.tools ?? []);
          const model = resolveModel(d.model, providers); environment.model = model.target;
          // A fixed model binding shares a stateless SDK factory within this node.
          const existing = cs.get(canonical(model.target));
          add({ definition: model.definition, dependencies: model.dependencies, config: null, owner: null, identity: existing?.identity ?? model });
          bindings.model = json({ ...model.binding, executionRef: model.target, bindingDigest: model.bindingDigest });
          used.push({ kind: "capacity", target: model.target, config: null });
          if ((d.tools?.length ?? 0) > 0 && model.binding.features.tools !== "supported") fail("Model does not support tools");
        }
        for (const t of d.tools ?? []) {
          const descriptor = toolDescriptor(t), target = generated("tool", descriptor);
          const def: CapacityDefinition = { ref: target, validateInput: validator(t.inputSchema), validateOutput: validator(t.outputSchema), async execute(input, ctx): Promise<import("../../../harness/src/index.js").Report<import("../../../harness/src/index.js").CapacityResult>> {
            try { return { payload: { kind: "succeeded", value: await t.execute(input, ctx) }, evidence: { tool: json(t.ref), actionID: ctx.actionID }, measurements: [] }; }
            catch (e) { if (e instanceof AgentExecutionError) return { payload: { kind: "failed", error: e.issue }, evidence: { tool: json(t.ref) }, measurements: [] }; throw e; }
          }, ...(t.reconcile ? { reconcile: t.reconcile } : t.effect.kind === "read" ? { reconcile: async () => ({ payload: { kind: "retryable" as const }, evidence: { readOnly: true, tool: json(t.ref) }, measurements: [] }) } : {}) };
          add({ definition: def, dependencies: { create: () => null }, config: null, owner: null, identity: t });
          environment.tools[t.name] = { target, config: null }; bindings[`tool:${t.name}`] = json({ ...descriptor, executionRef: target });
          used.push({ kind: "capacity", target, config: null });
        }
        for (const [alias, binding] of Object.entries(d.capabilities ?? {})) {
          const installation = installed.get(canonical(binding.ref)) ?? fail("Capability unavailable", "VERSION_UNAVAILABLE");
          const descriptor = installation.descriptor;
          if (!equal(binding.contract, descriptor.contract)) fail("Capability contract mismatch");
          const config = binding.config ?? null; validator(descriptor.configSchema)(config);
          const scope = descriptor.scope === "required" ? binding.scope ?? { kind: "run" as const } : null;
          if (descriptor.scope === "none" && binding.scope) fail("Unscoped capability cannot accept scope");
          const namespace = scope?.kind === "grant" ? grants[scope.name]?.namespace ?? fail("Scope grant unavailable", "FORBIDDEN") : null;
          const resolved = { descriptor, config, scope: scope ? { domainID, owner, kind: scope.kind, namespace } : null };
          const target = generated("capability", resolved);
          const context = (ctx: CapabilityExecutionContext<any>): CapabilityExecutionContext<any> => ({ ...ctx, scope: scope ? { domainID, principalID: owner, runID: ctx.runID, namespace: canonical([domainID, owner, scope.kind, scope.kind === "run" ? ctx.runID : namespace]) } : null });
          const output = validator(descriptor.outputSchema);
          const def: CapacityDefinition<any> = { ref: target, validateInput: validator(descriptor.inputSchema), validateOutput(value) { output(value); if (descriptor.contract.id === "dw.context.messages" && descriptor.contract.version === "1") contextResult(value); }, execute: (input, ctx) => installation.implementation.execute(input, context(ctx as CapabilityExecutionContext<any>)),
            ...(installation.implementation.reconcile ? { reconcile: (input: Json, ctx: any) => installation.implementation.reconcile!(input, context(ctx)) } : descriptor.effect.kind === "read" ? { reconcile: async () => ({ payload: { kind: "retryable" as const }, evidence: { readOnly: true, capability: json(descriptor.ref) }, measurements: [] }) } : {}) };
          add({ definition: def, dependencies: installation.dependencies, config, owner: scope ? owner : null, identity: installation });
          environment.capabilities[alias] = { target, contract: descriptor.contract, config };
          bindings[`capability:${alias}`] = json({ ...resolved, executionRef: target }); used.push({ kind: "capacity", target, config });
        }
        for (const target of Object.values(environment.delegates)) used.push({ kind: "agent", target, config: null });
        return { definition, options: d, environment, bindings, used };
      });
      type Gate = { kind: "capacity" | "agent"; target: Ref; config: Json; owner: string | null };
      const sortGates = (items: Gate[]) => {
        const unique = new Map<string, Gate>();
        for (const gate of items) unique.set(canonical(gate), gate);
        return [...unique.values()].sort((a, b) => canonical(a).localeCompare(canonical(b)));
      };
      const policyFor = (agentRef: Ref, items: Gate[]): HostPolicy => {
        const sorted = sortGates(items);
        return { ref: generated("policy", { domainID, agent: agentRef, gates: sorted }), authorize(request, ctx) {
          const gate = sorted.find(g => g.kind === request.kind && equal(g.target, request.target));
          // A gate only constrains its own targets. Sibling agents and plain
          // Drivers still meet every gate that actually covers the target.
          if (!gate) return true;
          return ctx.domainID === domainID && equal(gate.config, request.config) && (!gate.owner || gate.owner === ctx.principalID);
        } };
      };
      const perAgent = records.map(r => {
        const gates = sortGates(r.used.flatMap((u): Gate[] => {
          if (u.kind === "agent") return [{ kind: "agent" as const, target: u.target, config: null, owner: null }];
          const cap = cs.get(canonical(u.target));
          return cap ? [{ kind: "capacity" as const, target: cap.definition.ref, config: cap.config, owner: cap.owner }] : [];
        }));
        return { record: r, gates, policy: policyFor(r.definition.ref, gates) };
      });
      const mandatoryPolicies = [...new Map(perAgent.map(item => [canonical(item.policy.ref), item.policy])).values()];
      const agentsPlan: AgentDeploymentPlan["agents"] = [];
      const drivers = perAgent.map(({ record: r, policy }) => {
        const applied = [...(nodeOptions.policies ?? []), { ref: policy.ref, config: null }];
        const manifest = json({ sdk: "dw.agent/1", definition: r.definition.descriptor, bindings: r.bindings, policies: applied });
        r.environment.manifest = manifest; freeze(r.environment);
        const driver = r.options.strategy ? r.options.strategy.create(r.environment) : toolLoop(r.options, r.environment);
        if (!equal(driver.ref, r.definition.ref)) fail("Strategy Driver Ref mismatch");
        agentsPlan.push({ ref: r.definition.ref, manifest, manifestDigest: digest(manifest), businessDefinitions: [{ kind: "agent", ref: r.definition.ref }, ...r.used.map(u => ({ kind: u.kind, ref: u.target }))] });
        return initialize(driver, manifest, r.used);
      });
      const selectedPolicies = [...(nodeOptions.policies ?? []).map(policy => ({ ref: copy(policy.ref), config: copy(policy.config) })), ...perAgent.map(item => ({ ref: item.policy.ref, config: json({ "dw.agent.subject": item.record.definition.ref }) }))];
      compiled = { drivers, capacities: [...cs.values()], mandatoryPolicies, selectedPolicies };
      plan = freeze({ agents: agentsPlan, workerDefinitions: [...drivers.map(d => ({ kind: "agent" as const, ref: d.ref })), ...compiled.capacities.map(c => ({ kind: "capacity" as const, ref: c.definition.ref })), ...[...policies, ...mandatoryPolicies].map(p => ({ kind: "policy" as const, ref: p.ref }))], modelRetryTargets: compiled.capacities.filter(c => c.definition.ref.id === "dw.generated.model").map(c => c.definition.ref) });
      state = "prepared"; return copy(plan);
    } catch (e) { state = "failed"; throw e; }
  }
  return {
    registerAgent(definition) { collecting(); if (!agents.has(definition)) fail("Use defineAgent to create definitions"); if (definitions.some(d => equal(d.ref, definition.ref))) fail("Duplicate Agent Ref", "DUPLICATE_DEFINITION"); definitions.push(definition); },
    registerPolicy(policy) { collecting(); fields(policy, ["ref", "rewrite", "authorize"], ["ref"]); ref(policy.ref); if (policies.some(p => equal(p.ref, policy.ref))) fail("Duplicate Policy Ref", "DUPLICATE_DEFINITION"); if (policy.rewrite !== undefined && typeof policy.rewrite !== "function" || policy.authorize !== undefined && typeof policy.authorize !== "function") fail("Invalid policy"); policies.push({ ...policy, ref: copy(policy.ref) }); },
    prepare,
    async start() {
      if (state === "collecting") prepare();
      if (state !== "prepared") fail("Node cannot start in current state", "NODE_STATE_INVALID");
      state = "starting";
      try {
        underlying = session.createNode({ ...copy(nodeOptions), ...(onProgress ? { onProgress } : {}), ...(onMetric ? { onMetric } : {}), ...(onError ? { onError } : {}), policies: copy(compiled.selectedPolicies) });
        for (const p of [...policies, ...compiled.mandatoryPolicies]) underlying.registerPolicy(p);
        for (const c of compiled.capacities) underlying.registerCapacity(c.definition, c.dependencies);
        for (const d of compiled.drivers) underlying.registerAgent(d);
        await underlying.start(); state = "running";
      } catch (e) { state = "failed"; throw e; }
    },
    async stop(options) {
      if (!underlying) { state = "stopped"; return { stopped: true, remainingRunIDs: [], remainingActionIDs: [] }; }
      state = "stopping"; const result = await underlying.stop(options); if (result.stopped) state = "stopped"; return result;
    },
  };
}
