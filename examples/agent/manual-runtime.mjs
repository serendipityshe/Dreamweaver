import { randomUUID } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { connectHarness } from "@dreamweaver/harness";
import { createAgentClient, createAgentNode, defineAgent } from "@dreamweaver/harness/agent";
import { startCoordinator, StaticAuthentication } from "@dreamweaver/harness/runtime";

// No model account is needed: a custom strategy echoes a string deterministically.
const agent = defineAgent({ ref: { id: "example.minimal", version: "1" }, strategy: {
  ref: { id: "example.echo-strategy", version: "1" },
  create(environment) {
    const validate = value => { if (typeof value !== "string") throw new Error("Expected a string"); };
    return { ref: environment.agent, validateInput: validate, validateOutput: validate,
      advance({ run }) { return { kind: "complete", outcome: { kind: "succeeded", value: run.input }, remainingActions: "cancel" }; } };
  },
} });

const directory = resolve(process.argv[2] ?? ".artifacts/minimal-agent");
await mkdir(directory, { recursive: true });
// Local demo credentials last for this process; the principal identities stay stable.
const workerToken = randomUUID(), callerToken = randomUUID();
const workerGrant = { principalID: "local-worker", roles: ["worker"], operations: [], definitions: [],
  nodeIDs: ["minimal-worker"], nodeOperations: ["register", "renew", "claim", "report", "unregister"] };
const callerGrant = { principalID: "local-user", roles: ["client"],
  operations: ["client.submit", "client.getRun", "client.listActions"], definitions: [] };
const authentication = new StaticAuthentication([
  { token: workerToken, grant: workerGrant }, { token: callerToken, grant: callerGrant },
]);
const coordinator = await startCoordinator({ databasePath: resolve(directory, "harness.sqlite"), authentication });
let worker, caller;
try {
  worker = await connectHarness({ endpoint: coordinator.endpoint, tokenProvider: () => workerToken });
  const node = createAgentNode(worker, { node: { nodeID: "minimal-worker", concurrency: { runs: 1, actions: 1 } }, scopeOwner: "local-user" });
  node.registerAgent(agent);
  const plan = node.prepare();
  const grants = definitions => definitions.map(({ kind, ref }) => ({ kind, id: ref.id, versions: [ref.version] }));
  // The trusted host authorizes exactly the definitions it is about to install.
  authentication.replace([
    { token: workerToken, grant: { ...workerGrant, definitions: grants(plan.workerDefinitions) } },
    { token: callerToken, grant: { ...callerGrant, definitions: grants(plan.agents.flatMap(item => item.businessDefinitions)) } },
  ]);
  await node.start();
  caller = await connectHarness({ endpoint: coordinator.endpoint, tokenProvider: () => callerToken });
  const client = createAgentClient(caller.client);
  // The same key and input reuse the persisted task when this example runs again.
  const run = await client.agent(agent.ref).start({ requestKey: "minimal-echo-v1", input: "Hello from DreamWeaver" });
  const result = await run.wait({ timeoutMs: 30_000 });
  if (result.kind !== "completed" || result.outcome.kind !== "succeeded")
    throw new Error(`Agent did not succeed: ${JSON.stringify(result)}`);
  console.log(JSON.stringify({ runID: run.id, outcome: result.outcome }));
} finally {
  // Each resource closes even if an earlier close rejects.
  try { await caller?.close(); }
  finally { try { await worker?.close(); } finally { await coordinator.close(); } }
}
