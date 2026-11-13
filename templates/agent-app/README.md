# Agent 应用模板

需要 Node.js 24。日常开发主要改三个文件：

| 文件 | 放什么 |
| --- | --- |
| `agent.mjs` | 指令、模型选择、工具和 Agent 版本 |
| `app.config.mjs` | 模型服务、凭证来源和可选的本地运行配置 |
| `main.mjs` | 输入、调用和结果处理 |

## 开始

先在 SDK 仓库中执行 `npm ci` 和 `npm run build`。将整个模板目录复制到 SDK 仓库旁边的应用目录，例如 `my-agent`，然后进入应用目录执行下面的命令。

```powershell
npm install ../Dreamweaver
Copy-Item .env.example .env
Copy-Item gitignore .gitignore
```

在 `.env` 填写 `MODEL_API_KEY`、`MODEL_NAME`、非秘密的 `MODEL_ACCOUNT_ID` 和业务任务键 `AGENT_REQUEST_KEY`，按实际模型服务在 `app.config.mjs` 中选择 `openai-responses` 或 `anthropic-messages`。如果使用第三方服务，设置对应的 `MODEL_BASE_URL`；协议名表示实际请求协议，不会自动猜测服务兼容性。然后运行：

```powershell
npm start -- "用一句话解释什么是 Agent"
```

这会调用你配置的真实模型，并输出完整 `WaitResult`。成功完成时，回答位于 `result.outcome.value`，包含 `text`、`content` 和 `usage`。不配置模型、只验证 SDK 安装时，可在 SDK 项目运行 `npm run example:agent`。

如果安装的是 `.tgz`，同样可以从已安装包的 `templates/agent-app` 复制模板，将上述安装路径换成 `.tgz` 路径。

## 常见修改

添加工具：在 `agent.mjs` 中通过 `defineTool` 定义并放入 `tools`，同时在 `app.config.mjs` 的对应模型中配置 `{ features: { tools: "supported" } }`。默认只声明文本能力；SDK 不替供应商保证某型号支持工具。

修改存储或并发：给应用配置添加 `dataDirectory: "./data"` 或 `concurrency: { runs: 2, actions: 2 }`。默认目录为当前工作目录下的 `.dreamweaver`，并发为 4/4；目录自动创建。目录内的 SQLite 和报告队列应一起保留，一个目录由一个本地应用使用。

添加协作 Agent：配置 `additionalAgents: [anotherAgent]`，并在主 Agent 的 `delegates` 中引用其固定版本。可继续使用 `capabilities`、`grants` 和事件回调；分布式部署或多用户认证使用原有 `/agent`、`/runtime` 接口。

指令、工具、模型或其绑定配置变化时，为 Agent 设置新 `ref.version`；有未完成任务时，先保留其旧定义，通过 `additionalAgents` 一起注册。`MODEL_ACCOUNT_ID` 表示实际账户身份，不能填 API Key；同一账户换密钥不需要改变它。

## 任务身份和结果

`createAgentApp` 负责启动和资源管理，调用统一使用原有 `AgentClient → AgentHandle → RunHandle`。提交时必须显式传入并保存 `requestKey`，模板从 `AGENT_REQUEST_KEY` 读取，不自动生成。对于同一 Agent 和请求内容，同键复用任务，同键改变内容会报冲突；新业务任务使用新的键。

```js
const agent = app.client.agent(config.agent.ref);
const result = await agent.invoke({
  requestKey: "order-123-explanation-v1", input: "解释这份订单",
});

// 需要先保存任务身份、随后再等待时：
const run = await agent.start({
  requestKey: "order-456-explanation-v1", input: "解释另一份订单",
});
console.log(run.id, run.requestKey);
const observed = await run.wait();
// 重启后：app.client.run(runID).wait()
// 明确取消：run.cancel({ requestKey: "cancel-order-456-v1", reason: "用户取消" })
```

`invoke` 等价于提交后等待；`invoke/wait` 返回结构化状态，不将业务失败、取消、阻塞或等待超时改成异常。RPC 错误或等待中止通过原有 `AgentClientError` 表达，保留已知任务身份和原因。提交响应不明时保留原请求键核对，不更换键创建替代任务。

```js
const result = await agent.invoke(
  { requestKey: "long-task-1", input: "一个较长的任务" },
  { wait: { timeoutMs: 120_000 } },
);
if (result.kind === "completed") {
  // outcome.kind 区分 succeeded、failed、cancelled；completed 不只表示成功。
  console.log(result.run.id, result.outcome);
} else if (result.kind === "blocked") {
  console.log(result.run.id, result.issues, result.actionIDs);
} else {
  console.log(result.runID, result.lastObserved); // timeout，只结束本次等待。
}
```

默认等待 60 秒；超时或中止等待不取消任务，也不重新提交。模型结果不明时保留阻塞状态，不自动接受重复执行风险。`close()` 可重复调用，会中断本地执行并释放资源，保留持久任务；返回的 `stopped: false` 表示仍有执行未在关闭期限内退出，原始任务身份保留在返回值中。模板中的 `finally` 负责正常退出时的清理。

取消请求被接受不等于任务已经结束。`run.get()` 返回的 `outcome` 才表示终态；取消推进期间，`run.wait()` 仍可能观察到原来的阻塞状态。
