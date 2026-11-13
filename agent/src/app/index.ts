import { randomUUID } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { connectHarness, type HarnessSession, type NodeStopResult } from "../../../harness/src/index.js";
import { startCoordinator, StaticAuthentication, type Coordinator, type PrincipalGrant } from "../../../harness/src/runtime.js";
import { createAgentClient } from "../client/client.js";
import { createAgentNode } from "../definition/node.js";
import { agents } from "../definition/definitions.js";
import { canonical, copy, fail, fields, positive, string } from "../contract/common.js";
import type { AgentApp, AgentAppOptions } from "./types.js";
import { appProviders } from "./providers.js";

export type * from "./types.js";

/** Starts a persistent, authenticated loopback coordinator and its Agent worker as one owned resource. */
export async function createAgentApp(options: AgentAppOptions): Promise<AgentApp> {
  fields(options, ["agent", "additionalAgents", "dataDirectory", "providers", "capabilities", "grants", "concurrency", "onProgress", "onMetric", "onError"], ["agent"]);
  if (options.additionalAgents !== undefined && !Array.isArray(options.additionalAgents)) fail("additionalAgents must be an array");
  const definitions = [options.agent, ...(options.additionalAgents ?? [])];
  const refs = new Set<string>();
  for (const definition of definitions) {
    if (!agents.has(definition)) fail("Use defineAgent to create the app's Agents");
    const key = canonical(definition.ref);
    if (refs.has(key)) fail("Duplicate Agent Ref", "DUPLICATE_DEFINITION");
    refs.add(key);
  }
  // A local app must install all delegates; otherwise a typo would leave a task waiting indefinitely.
  for (const definition of definitions) for (const target of Object.values(agents.get(definition)!.delegates ?? {})) {
    if (!refs.has(canonical(target))) fail(`Delegate ${target.id}@${target.version} is not installed in this app`, "VERSION_UNAVAILABLE");
  }
  const providers = appProviders(options.providers);
  const concurrency = copy(options.concurrency ?? { runs: 4, actions: 4 });
  fields(concurrency, ["runs", "actions"], ["runs", "actions"]); positive(concurrency.runs); positive(concurrency.actions);
  for (const callback of [options.onProgress, options.onMetric, options.onError]) if (callback !== undefined && typeof callback !== "function") fail("Invalid app event callback");
  const directory = options.dataDirectory ?? ".dreamweaver"; string(directory);
  const dataDirectory = resolve(directory);
  const capabilities = [...(options.capabilities ?? [])], grants = copy(options.grants ?? {});
  const { onProgress, onMetric, onError } = options;
  await mkdir(dataDirectory, { recursive: true });

  const workerToken = randomUUID(), callerToken = randomUUID();
  // Principals and node identity stay stable across restarts of this data directory.
  const workerGrant: PrincipalGrant = { principalID: "local-worker", roles: ["worker"], operations: [], definitions: [],
    nodeIDs: ["local-agent-worker"], nodeOperations: ["register", "renew", "claim", "report", "unregister"] };
  const callerGrant: PrincipalGrant = { principalID: "local-user", roles: ["client"], definitions: [],
    operations: ["client.submit", "client.getRun", "client.listActions", "client.requestCancel"] };
  const authentication = new StaticAuthentication([{ token: workerToken, grant: workerGrant }, { token: callerToken, grant: callerGrant }]);
  let coordinator: Coordinator | undefined, worker: HarnessSession | undefined, caller: HarnessSession | undefined;
  let closing: Promise<NodeStopResult> | undefined;
  function close(): Promise<NodeStopResult> {
    return closing ??= (async () => {
      let stopped: NodeStopResult = { stopped: true, remainingRunIDs: [], remainingActionIDs: [] };
      const errors: unknown[] = [];
      try { if (worker) stopped = await worker.close(); } catch (error) { errors.push(error); }
      try { await caller?.close(); } catch (error) { errors.push(error); }
      try { await coordinator?.close(); } catch (error) { errors.push(error); }
      if (errors.length) throw new AggregateError(errors, "Agent app cleanup failed");
      return stopped;
    })();
  }
  try {
    coordinator = await startCoordinator({ databasePath: join(dataDirectory, "harness.sqlite"), authentication });
    worker = await connectHarness({ endpoint: coordinator.endpoint, tokenProvider: () => workerToken });
    const node = createAgentNode(worker, { node: { nodeID: "local-agent-worker", concurrency,
      reportSpool: { directory: join(dataDirectory, "reports"), maxBytes: 64 * 1024 * 1024 },
      ...(onProgress ? { onProgress } : {}), ...(onMetric ? { onMetric } : {}), ...(onError ? { onError } : {}) },
      providers, capabilities, grants, scopeOwner: callerGrant.principalID });
    definitions.forEach(definition => node.registerAgent(definition));
    const plan = node.prepare();
    const permissions = (definitions: typeof plan.workerDefinitions) => definitions.map(({ kind, ref }) => ({ kind, id: ref.id, versions: [ref.version] }));
    authentication.replace([
      { token: workerToken, grant: { ...workerGrant, definitions: permissions(plan.workerDefinitions) } },
      { token: callerToken, grant: { ...callerGrant, definitions: permissions(plan.agents.flatMap(agent => agent.businessDefinitions)) } },
    ]);
    await node.start();
    caller = await connectHarness({ endpoint: coordinator.endpoint, tokenProvider: () => callerToken });
    const client = createAgentClient(caller.client);
    return Object.freeze({ dataDirectory, client, close });
  } catch (error) {
    try { await close(); } catch (cleanup) { throw new AggregateError([error, cleanup], "Agent app startup and cleanup failed", { cause: error }); }
    throw error;
  }
}
