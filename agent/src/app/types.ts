import type { NodeOptions, NodeStopResult } from "../../../harness/src/index.js";
import type { AgentClient, AgentDefinition, AgentNodeOptions, ProviderConfiguration } from "../contract/types.js";

/** Text-only by default; declare tools: "supported" when the selected model supports them. */
export interface AppProviderConfiguration extends Omit<ProviderConfiguration, "revision" | "protocol" | "models"> {
  revision?: string;
  protocol: "openai-responses" | "anthropic-messages";
  models: Record<string, Omit<ProviderConfiguration["models"][string], "features"> & {
    features?: Partial<ProviderConfiguration["models"][string]["features"]>;
  }>;
}

/** One trusted local application, with an initial Agent and optional collaborators. */
export interface AgentAppOptions {
  agent: AgentDefinition;
  additionalAgents?: AgentDefinition[];
  /** Defaults to .dreamweaver in the working directory; created automatically. */
  dataDirectory?: string;
  providers?: Record<string, AppProviderConfiguration>;
  capabilities?: AgentNodeOptions["capabilities"];
  grants?: AgentNodeOptions["grants"];
  /** Defaults to { runs: 4, actions: 4 }. */
  concurrency?: NodeOptions["concurrency"];
  onProgress?: NodeOptions["onProgress"];
  onMetric?: NodeOptions["onMetric"];
  onError?: NodeOptions["onError"];
}

export interface AgentApp {
  readonly dataDirectory: string;
  /** Standard AgentClient: select an Agent, submit with an explicit requestKey, and track runs. */
  readonly client: AgentClient;
  /** Interrupts local execution and closes resources. Does not cancel or delete persisted tasks. */
  close(): Promise<NodeStopResult>;
}
