import { createAgentApp } from "@dreamweaver/harness/app";
import config from "./app.config.mjs";

const requestKey = process.env.AGENT_REQUEST_KEY;
if (!requestKey?.trim()) throw new Error("请在 .env 中设置 AGENT_REQUEST_KEY；同一业务任务使用同一个键。");

const app = await createAgentApp(config);
try {
  const agent = app.client.agent(config.agent.ref);
  const result = await agent.invoke({
    requestKey,
    input: process.argv[2] ?? "你好，请介绍一下你能做什么。",
  });
  console.log(JSON.stringify(result, null, 2));
} finally {
  await app.close();
}
