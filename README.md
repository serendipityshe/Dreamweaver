# DreamWeaver

DreamWeaver 提供 Agent 定义、模型与工具接入、任务执行、持久查询和恢复能力。SDK 包名为 `@dreamweaver/harness`，使用 Node.js 24 和 ESM。

## 快速开始

```sh
git clone https://github.com/serendipityshe/Dreamweaver.git
cd Dreamweaver
npm ci
npm run example:agent
```

最小示例运行一个离线 Echo Agent，经过本地 HTTP 和 SQLite，输出完整的 `WaitResult`。成功结果中的 `outcome.value` 为 `Hello from DreamWeaver`。数据保存在 `.artifacts/minimal-agent/`，重复运行会通过固定请求键复用同一任务。

## 在应用中调用

先在 SDK 仓库执行 `npm run build`，再在相邻的应用目录安装 SDK。

```sh
npm install ../Dreamweaver
```

```js
import { createAgentApp } from "@dreamweaver/harness/app";
import config from "./app.config.mjs";

const app = await createAgentApp(config);
try {
  const agent = app.client.agent(config.agent.ref);
  const result = await agent.invoke({
    requestKey: "hello-001",
    input: "你好",
  });
  console.log(result);
} finally {
  await app.close();
}
```

真实模型应用从模板开始。`agent.mjs` 定义 Agent、模型和工具，`app.config.mjs` 配置模型服务及凭证来源，`main.mjs` 负责调用和结果处理。模板支持配置 `openai-responses` 或 `anthropic-messages` 协议。[应用模板](templates/agent-app/README.md)

`createAgentApp` 默认使用 `.dreamweaver` 持久目录、本机回环地址和可用端口，自动装配认证、权限及执行节点。运行与动作并发默认均为 4。

## 任务身份与结果

调用使用 `AgentClient → AgentHandle → RunHandle` 接口。每个业务任务需要显式提供并保存 `requestKey`，同键同内容复用任务，新任务使用新的键。

```js
const agent = app.client.agent(config.agent.ref);
const run = await agent.start({ requestKey: "order-123", input: "处理订单" });
console.log(run.id);
const result = await run.wait();

// 通过保存的任务 ID 恢复查询或等待。
const resumed = app.client.run(run.id);
const current = await resumed.get();
```

`invoke` 提交任务并等待结果。`invoke/wait` 返回 `completed`、`blocked` 或 `timeout`。完成结果通过 `outcome.kind` 区分 `succeeded`、`failed` 和 `cancelled`。默认等待 60 秒，等待超时后可继续查询原任务。取消使用 `run.cancel({ requestKey, reason })`，完成情况通过任务结果确认。

## 公开入口

| 导入路径 | 用途 |
| --- | --- |
| `@dreamweaver/harness/app` | 启动本地应用、取得 AgentClient、关闭资源 |
| `@dreamweaver/harness/agent` | 定义 Agent、装配节点、调用任务 |
| `@dreamweaver/harness/model` | 模型适配契约与能力 |
| `@dreamweaver/harness/runtime` | 启动协调器、配置认证、检查恢复状态 |
| `@dreamweaver/harness` | 连接协调器、提交和查询任务、创建执行节点 |

底层宿主装配包括协调器、认证、执行节点和调用方的生命周期管理。[手动装配示例](examples/agent/manual-runtime.mjs)

## 构建与打包

```sh
npm run typecheck
npm pack
```

`npm pack` 自动构建，并生成 `dreamweaver-harness-0.1.0.tgz`。其他项目可以通过 `npm install /path/to/dreamweaver-harness-0.1.0.tgz` 安装。安装包包含编译后的代码、类型声明、最小示例和应用模板。当前包设置为 `private`，公共 npm 发布尚待配置。

第三方依赖及来源说明随仓库提供。[第三方声明](THIRD_PARTY_NOTICES.md)
