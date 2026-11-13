import { defineAgent } from "@dreamweaver/harness/agent";

if (!process.env.MODEL_NAME) throw new Error("请在 .env 中设置 MODEL_NAME");

export default defineAgent({
  ref: { id: "my-assistant", version: "1" },
  instructions: "用中文简洁回答用户的问题。",
  model: { provider: "default", name: process.env.MODEL_NAME },
  // Add tools created with defineTool here; enable their model support in app.config.mjs.
  tools: [],
});
