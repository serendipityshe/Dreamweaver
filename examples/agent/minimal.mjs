import { createAgentApp } from "@dreamweaver/harness/app";
import { echoAgent } from "./echo-agent.mjs";

// Offline smoke example. For a model-backed Agent, use templates/agent-app.
const app = await createAgentApp({
  agent: echoAgent,
  dataDirectory: process.argv[2] ?? ".artifacts/minimal-agent",
});
try {
  const agent = app.client.agent(echoAgent.ref);
  const result = await agent.invoke({ requestKey: "minimal-echo-v1", input: "Hello from DreamWeaver" });
  console.log(JSON.stringify(result));
} finally {
  await app.close();
}
