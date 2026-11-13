import type { Json, Ref, VersionSelector, Issue, RunView, Outcome, Report, CapacityResult, Reconciliation, AgentDriver, CapacityContext, DependencyBinding, HostPolicy, NodeOptions, NodeStopResult, RunLimits, CallOptions } from "../../../harness/src/index.js";
import type { ModelFeatures, ModelMessage } from "../../../capacities/model/contract/types.js";
import type { JsonSchema } from "./schema.js";

export interface ModelSelection {
  provider: string;
  name: string;
  parameters?: { temperature?: number; maxOutputTokens?: number; stop?: string[] };
  providerOptions?: { [key: string]: Json };
}
export interface CapabilityBinding {
  ref: Ref;
  contract: Ref;                 // 领域输入输出契约，不是实现 Ref
  config?: Json;                 // 省略规范化为 null
  scope?: { kind: "run" } | { kind: "grant"; name: string };
}
export interface AgentDefinitionOptions {
  ref: Ref;
  instructions?: string;        // 默认 ""
  model?: ModelSelection;       // 默认策略必需；自定义策略可不用模型
  tools?: ToolDefinition[];
  capabilities?: Record<string, CapabilityBinding>;
  context?: string[];           // capabilities 的别名，按顺序调用
  delegates?: Record<string, Ref>;
  strategy?: StrategyDefinition;
  limits?: {
    maxModelCalls?: number;      // 默认 16，已提交的逻辑模型 Action 数
    maxToolCallsPerRound?: number; // 默认 8
    maxTranscriptBytes?: number; // 默认 65536，规范化 JSON 的 UTF-8 字节数
  };
}
export interface AgentDefinition {
  readonly ref: Ref;
  readonly descriptor: Json;    // 可序列化的行为声明；无函数、秘密
}

export type Effect =
  | { kind: "read" }
  | { kind: "idempotent"; guarantee: string }
  | { kind: "external" };

export interface ToolDefinition {
  ref: Ref;
  name: string;
  description: string;
  inputSchema: JsonSchema;
  outputSchema: JsonSchema;
  effect: Effect;
  execute(input: Json, ctx: CapacityContext): Promise<Json>;
  reconcile?(input: Json, ctx: CapacityContext): Promise<Report<Reconciliation>>;
}

export interface CapabilityDescriptor {
  ref: Ref;
  contract: Ref;
  configSchema: JsonSchema;
  inputSchema: JsonSchema;
  outputSchema: JsonSchema;
  effect: Effect;
  scope: "none" | "required";
}
export interface ResolvedScope {
  domainID: string;
  principalID: string;
  namespace: string;
  runID: string;
}
export interface CapabilityExecutionContext<TDeps> extends CapacityContext<TDeps> {
  scope: ResolvedScope | null;
}
export interface CapabilityImplementation<TDeps> {
  execute(input: Json, ctx: CapabilityExecutionContext<TDeps>): Promise<Report<CapacityResult>>;
  reconcile?(input: Json, ctx: CapabilityExecutionContext<TDeps>): Promise<Report<Reconciliation>>;
}
export interface CapabilityInstallation<TDeps = unknown> {
  descriptor: CapabilityDescriptor;
  implementation: CapabilityImplementation<TDeps>;
  dependencies: DependencyBinding<TDeps>;
}
export interface InstalledCapability {
  readonly descriptor: CapabilityDescriptor;
}

export interface ProviderConfiguration {
  revision: string;
  protocol: "openai-responses" | "anthropic-messages" | "openai-chat-completions";
  baseURL?: string;
  allowInsecureHTTP?: boolean;   // 仅严格为 true 时允许显式 HTTP 地址
  accountIdentity: string;       // 非秘密的账户/租户标识
  apiKey: string | (() => string | Promise<string>);
  models: Record<string, {
    features: ModelFeatures;
    parameters?: ModelSelection["parameters"];
    providerOptions?: { [key: string]: Json };
  }>;
}
export interface ScopeGrant {
  namespace: string;            // 宿主批准的固定数据域
}
export interface AgentNodeOptions {
  node: NodeOptions;
  providers?: Record<string, ProviderConfiguration>;
  capabilities?: InstalledCapability[];
  scopeOwner?: string;          // 默认 session.binding.principalID
  grants?: Record<string, ScopeGrant>;
}
export interface DeploymentDefinition {
  kind: "agent" | "capacity";
  ref: Ref;
}
export interface AgentDeploymentPlan {
  agents: {
    ref: Ref;
    manifest: Json;
    manifestDigest: string;
    businessDefinitions: DeploymentDefinition[];
  }[];
  workerDefinitions: { kind: "agent" | "capacity" | "policy"; ref: Ref }[];
  modelRetryTargets: Ref[];       // 可供宿主审阅的模型执行 Ref；不自动授权
}
export interface AgentNode {
  registerAgent(definition: AgentDefinition): void;
  registerPolicy(policy: HostPolicy): void;
  prepare(): AgentDeploymentPlan;
  start(): Promise<void>;
  stop(options: { mode: "drain" | "interrupt"; timeoutMs: number }): Promise<NodeStopResult>;
}

export interface StrategyEnvironment {
  agent: Ref;
  manifest: Json;
  model: Ref | null;
  capabilities: Record<string, {
    target: Ref; contract: Ref; config: Json;
  }>;
  tools: Record<string, { target: Ref; config: Json }>;
  delegates: Record<string, Ref>;
}
export interface StrategyDefinition {
  ref: Ref;
  create(environment: StrategyEnvironment): AgentDriver;
}

export interface ContextRequest {
  task: string;
  messages: ModelMessage[];
  maxOutputBytes: number;
}
export interface ContextResult {
  messages: ModelMessage[];
  sources: ContentReference[];
}
export interface ContentReference {
  resolver: Ref;                 // 解析能力及固定版本
  scope: string;                 // 不透明数据域标识，不是权限
  key: string;
  revision: string;
  sha256: string;
  mediaType: string;
  bytes: number;
}

export interface StartAgentRequest {
  requestKey: string;
  input: Json;
  limits?: Partial<RunLimits>;
}
export interface WaitOptions {
  timeoutMs?: number;            // 默认 60000；有限非负安全整数，0 表示一次立即查询
  signal?: AbortSignal;
  poll?: { initialMs?: number; maxMs?: number }; // 默认 250 / 1000
}
export type WaitResult =
  | { kind: "completed"; run: RunView; outcome: Outcome }
  | { kind: "blocked"; run: RunView; issues: Issue[]; actionIDs: string[] }
  | { kind: "timeout"; runID: string; lastObserved: RunView | null };
export interface RunHandle {
  readonly id: string;
  readonly requestKey: string | null;
  get(options?: CallOptions): Promise<RunView>;
  wait(options?: WaitOptions): Promise<WaitResult>;
  cancel(request: { requestKey: string; reason?: string }, options?: CallOptions): Promise<RunView>;
}
export interface AgentHandle {
  start(request: StartAgentRequest, options?: CallOptions): Promise<RunHandle>;
  invoke(request: StartAgentRequest,
    options?: { submit?: CallOptions; wait?: WaitOptions }): Promise<WaitResult>;
}
export interface AgentClient {
  agent(ref: VersionSelector): AgentHandle;
  run(runID: string): RunHandle;
}
