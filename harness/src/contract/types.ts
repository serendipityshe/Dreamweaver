// Shared public contract types: CONTRACT v0.4 sections 2 and 12. T03 implements connection and basic queries.
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Timestamp = number;
export type CoreState = "started" | "suspended" | "resumed" | "completed";
export type DefinitionKind = "agent" | "capacity";

export interface Ref { id: string; version: string }
export interface VersionSelector { id: string; version?: string }
export interface Issue { code: string; message: string; details: Json }
export interface Continuation { entry: string; state: Json }

export type Outcome =
  | { kind: "succeeded"; value: Json }
  | { kind: "failed"; error: Issue }
  | { kind: "cancelled"; reason: string | null };
export type CapacityResult = Extract<Outcome, { kind: "succeeded" | "failed" }>;
export interface MetricSample { name: string; value: number; unit: string | null }
export interface Report<T> {
  payload: T;
  evidence: Json;
  measurements: MetricSample[];
}
export interface CancellationInfo {
  source: "client" | "parent" | "completion" | "agent";
  reason: string | null;
  requestedAt: Timestamp;
}

export interface RunLimits {
  maxDepth: number;
  maxActions: number;
  maxFrames: number;
  maxExecutionsPerAction: number;
  defaultWaitTimeoutMs: number | null;
}
export interface CallOptions { signal?: AbortSignal; timeoutMs?: number }
export interface PageRequest { cursor?: string; limit?: number }
export interface Page<T> { items: T[]; nextCursor: string | null }

export type ErrorCode =
  | "INVALID_ARGUMENT" | "UNAUTHENTICATED" | "FORBIDDEN"
  | "PROTOCOL_MISMATCH" | "SESSION_MISMATCH" | "NOT_FOUND" | "REQUEST_CONFLICT"
  | "REVISION_CONFLICT" | "STATE_CONFLICT" | "VERSION_UNAVAILABLE" | "DUPLICATE_DEFINITION"
  | "LIMIT_EXCEEDED" | "RUN_TERMINAL" | "ACTION_TERMINAL"
  | "EXECUTION_FENCED" | "RECONCILIATION_REQUIRED" | "CHECKPOINT_BUSY"
  | "NODE_STATE_INVALID" | "INVALID_DECISION" | "OUTPUT_VALIDATION_FAILED"
  | "CALL_ABORTED" | "TRANSPORT_UNCERTAIN" | "INTERNAL_ERROR";
export interface ErrorInfo {
  code: ErrorCode;
  message: string;
  disposition: "rejected" | "uncertain";
  details: Json;
  retryAfterMs: number | null;
}

export interface SessionBinding { domainID: string; principalID: string }
export interface DomainInfo extends SessionBinding {
  protocolVersion: "harness/1";
  defaultLimits: RunLimits;
  maxPayloadBytes: number;
  maxValueBytes: number;
  maxFrameBytes: number;
  maxResponseBytes: number;
  maxPageSize: number;
  idempotencyRetention: "until-domain-reset";
}

export interface ActionRequest {
  key: string;
  kind: DefinitionKind;
  target: VersionSelector;
  input: Json;
  config: Json;
}
export interface AppliedPolicy { ref: Ref; config: Json }
export interface PreparedActionRequest {
  key: string;
  kind: DefinitionKind;
  target: Ref;
  input: Json;
  config: Json;
  policies: AppliedPolicy[];
}
export interface Checkpoint { revision: number; value: Json }
export interface AttemptView {
  id: string;
  number: number;
  nodeID: string;
  recovering: boolean;
  mode: "automatic-recovery" | "recheck-only";
  executeDispatched: boolean;
  startedAt: Timestamp;
  finishedAt: Timestamp | null;
}
export interface ActionObservation {
  id: string;
  attemptID: string | null;
  source: "execute" | "reconcile" | "operator" | "remote";
  observedAt: Timestamp;
  result: Outcome | null;
  evidence: Json;
  validationError: Issue | null;
  measurements: MetricSample[];
  reported: Json;
  validated: boolean;
  acceptedAsOutcome: boolean;
}
export interface ActionView {
  id: string;
  runID: string;
  revision: number;
  original: ActionRequest;
  request: PreparedActionRequest;
  status: "accepted" | "running" | "settled";
  latestAttempt: AttemptView | null;
  executionCount: number;
  checkpoint: Checkpoint;
  outcome: Outcome | null;
  outcomeObservationID: string | null;
  cancelRequested: boolean;
  cancellation: CancellationInfo | null;
  blocked: Issue | null;
  recheckRequested: boolean;
  remoteRunID: string | null;
  latestObservation: ActionObservation | null;
  createdAt: Timestamp;
  updatedAt: Timestamp;
  settledAt: Timestamp | null;
}
export interface WaitView {
  id: string;
  actionKeys: string[];
  deadlineAt: Timestamp | null;
  winner: "results" | "timeout" | null;
}
export interface RunView {
  id: string;
  rootID: string;
  parentActionID: string | null;
  revision: number;
  agent: Ref;
  input: Json;
  config: Json;
  lifecycle: CoreState | null;
  continuation: Continuation;
  wait: WaitView | null;
  outcome: Outcome | null;
  cancelRequested: boolean;
  cancellation: CancellationInfo | null;
  blocked: Issue | null;
  limits: RunLimits;
  rootActionCount: number;
  frameCount: number;
  createdAt: Timestamp;
  updatedAt: Timestamp;
  completedAt: Timestamp | null;
}

export interface SubmitRequest {
  requestKey: string;
  agent: VersionSelector;
  input: Json;
  config: Json;
  limits?: Partial<RunLimits>;
}
export interface SubmitReceipt { runID: string; acceptedAt: Timestamp; agent: Ref }
export interface CancelRequest { requestKey: string; runID: string; reason: string | null }
export interface VersionQuery { kind: DefinitionKind; id: string }
export interface VersionPointer {
  kind: DefinitionKind;
  id: string;
  ref: Ref | null;
  revision: number;
  evidence: Json;
  updatedAt: Timestamp | null;
}
export type CommandOperation = "submit" | "cancel" | "resumeRun" | "recheckAction" | "resolveAction" | "adoptVersion";
export interface CommandView {
  requestKey: string;
  operation: CommandOperation;
  status: "pending" | "applied" | "rejected";
  result: Json | null;
  error: ErrorInfo | null;
  createdAt: Timestamp;
  updatedAt: Timestamp;
}
export type HistorySubject =
  | { kind: "run"; runID: string }
  | { kind: "action"; actionID: string }
  | { kind: "version"; definitionKind: DefinitionKind; id: string };
export interface HistoryRequest extends PageRequest { subject: HistorySubject }
export interface HistoryRecord {
  id: string;
  sequence: number;
  at: Timestamp;
  runID: string | null;
  actionID: string | null;
  attemptID: string | null;
  type: string;
  data: Json;
}
export interface HarnessClient {
  submit(request: SubmitRequest, options?: CallOptions): Promise<SubmitReceipt>;
  getRun(request: { runID: string }, options?: CallOptions): Promise<RunView>;
  getRunByRequestKey(request: { requestKey: string }, options?: CallOptions): Promise<RunView | null>;
  getAction(request: { actionID: string }, options?: CallOptions): Promise<ActionView>;
  listActions(request: PageRequest & { runID: string }, options?: CallOptions): Promise<Page<ActionView>>;
  requestCancel(request: CancelRequest, options?: CallOptions): Promise<RunView>;
  getCommand(request: { requestKey: string }, options?: CallOptions): Promise<CommandView | null>;
  getVersion(request: VersionQuery, options?: CallOptions): Promise<VersionPointer>;
  getHistory(request: HistoryRequest, options?: CallOptions): Promise<Page<HistoryRecord>>;
}

export interface Fact { id: string; actionID: string; actionKey: string; outcome: Outcome }
export type Wake =
  | { kind: "start" }
  | { kind: "results"; waitID: string; facts: Fact[] }
  | { kind: "timeout"; waitID: string; facts: Fact[]; pendingActionKeys: string[] };
export interface AgentRunView {
  id: string;
  rootID: string;
  parentActionID: string | null;
  agent: Ref;
  input: Json;
  config: Json;
  limits: RunLimits;
}
export interface AdvanceInput {
  frameID: string;
  run: AgentRunView;
  continuation: Continuation;
  wake: Wake;
}
export type Decision =
  | {
      kind: "dispatch";
      continuation: Continuation;
      actions: ActionRequest[];
      wait: { all: string[]; timeoutMs?: number | null };
    }
  | { kind: "complete"; outcome: Outcome; remainingActions: "cancel" };
export interface AgentDriver {
  ref: Ref;
  validateInput(input: Json): void;
  validateOutput(output: Json): void;
  advance(input: AdvanceInput): Decision | Promise<Decision>;
}

export interface CapacityContext<TDeps = unknown> {
  runID: string;
  actionID: string;
  attemptID: string;
  attemptNumber: number;
  signal: AbortSignal;
  config: Json;
  deps: TDeps;
  checkpoint: Checkpoint;
  saveCheckpoint(value: Json): Promise<Checkpoint>;
  reportProgress(value: Json): void;
  reportMetric(sample: MetricSample): void;
}
export type Reconciliation =
  | { kind: "settled"; result: CapacityResult }
  | { kind: "retryable" }
  | { kind: "unresolved"; reason: Issue };
export interface CapacityDefinition<TDeps = unknown> {
  ref: Ref;
  validateInput(input: Json): void;
  validateOutput(output: Json): void;
  execute(input: Json, context: CapacityContext<TDeps>): Promise<Report<CapacityResult>>;
  reconcile?(input: Json, context: CapacityContext<TDeps>): Promise<Report<Reconciliation>>;
}
export interface BindingContext {
  runID: string;
  actionID: string;
  attemptID: string;
  phase: "execute" | "reconcile";
  capacity: Ref;
  config: Json;
  signal: AbortSignal;
}
export interface DependencyBinding<TDeps> {
  create(context: BindingContext): TDeps | Promise<TDeps>;
  dispose?(deps: TDeps): void | Promise<void>;
}
export interface PolicyContext {
  domainID: string;
  principalID: string;
  runID: string;
  rootID: string;
  config: Json;
  phase: "admission" | "execution" | "reconciliation";
}
export interface HostPolicy {
  ref: Ref;
  rewrite?(request: ActionRequest, context: PolicyContext): ActionRequest;
  authorize?(request: PreparedActionRequest, context: PolicyContext): boolean | Promise<boolean>;
}
export interface ProgressEvent {
  runID: string;
  actionID: string;
  attemptID: string;
  at: Timestamp;
  value: Json;
}
export interface MetricEvent extends Omit<ProgressEvent, "value"> { sample: MetricSample }
export interface NodeOptions {
  nodeID: string;
  concurrency: { runs: number; actions: number };
  /** Optional node-local durable report queue and logical byte budget. */
  reportSpool?: { directory: string; maxBytes: number };
  policies?: AppliedPolicy[];
  onProgress?: (event: ProgressEvent) => void;
  onMetric?: (event: MetricEvent) => void;
  onError?: (error: ErrorInfo) => void;
}
export interface NodeStopResult {
  stopped: boolean;
  remainingRunIDs: string[];
  remainingActionIDs: string[];
}
export interface HarnessNode {
  registerAgent(driver: AgentDriver): void;
  registerCapacity<TDeps>(definition: CapacityDefinition<TDeps>, binding: DependencyBinding<TDeps>): void;
  registerPolicy(policy: HostPolicy): void;
  start(): Promise<void>;
  stop(options: { mode: "drain" | "interrupt"; timeoutMs: number }): Promise<NodeStopResult>;
}

export interface ResumeRunRequest {
  requestKey: string;
  runID: string;
  expectedRevision: number;
  evidence: Json;
}
export interface RecheckActionRequest {
  requestKey: string;
  actionID: string;
  expectedRevision: number;
  reason: string;
}
export type ActionResolution =
  | { kind: "settle"; result: CapacityResult; measurements: MetricSample[] }
  | { kind: "allow_retry" }
  | { kind: "allow_model_retry"; acceptDuplicateCharges: true }
  | { kind: "legacy_report"; reportID: string; attemptID: string; phase: "execute" | "reconcile"; reportDigest: string;
      maxValueBytes: number; basis: "recorded_configuration" | "administrator_approval"; source: string; scope: string };
export interface ResolveActionRequest {
  requestKey: string;
  actionID: string;
  expectedRevision: number;
  resolution: ActionResolution;
  evidence: Json;
}
export interface AdoptVersionRequest {
  requestKey: string;
  kind: DefinitionKind;
  ref: Ref;
  expectedRevision: number;
  evidence: Json;
}
export interface HarnessAdmin {
  resumeRun(request: ResumeRunRequest, options?: CallOptions): Promise<RunView>;
  recheckAction(request: RecheckActionRequest, options?: CallOptions): Promise<ActionView>;
  resolveAction(request: ResolveActionRequest, options?: CallOptions): Promise<ActionView>;
  adoptVersion(request: AdoptVersionRequest, options?: CallOptions): Promise<VersionPointer>;
}
export interface ConnectOptions {
  endpoint: string;
  tokenProvider: () => string | Promise<string>;
}
export interface HarnessSession {
  readonly binding: SessionBinding;
  getInfo(options?: CallOptions): Promise<DomainInfo>;
  client: HarnessClient;
  admin: HarnessAdmin;
  createNode(options: NodeOptions): HarnessNode;
  close(options?: { timeoutMs?: number }): Promise<NodeStopResult>;
}
export type ConnectHarness = (options: ConnectOptions) => Promise<HarnessSession>;

export interface PublicRPCMap {
  "session.getInfo": { params: Record<string, never>; result: DomainInfo };
  "client.submit": { params: SubmitRequest; result: SubmitReceipt };
  "client.getRun": { params: { runID: string }; result: RunView };
  "client.getRunByRequestKey": { params: { requestKey: string }; result: RunView | null };
  "client.getAction": { params: { actionID: string }; result: ActionView };
  "client.listActions": { params: PageRequest & { runID: string }; result: Page<ActionView> };
  "client.requestCancel": { params: CancelRequest; result: RunView };
  "client.getCommand": { params: { requestKey: string }; result: CommandView | null };
  "client.getVersion": { params: VersionQuery; result: VersionPointer };
  "client.getHistory": { params: HistoryRequest; result: Page<HistoryRecord> };
  "admin.resumeRun": { params: ResumeRunRequest; result: RunView };
  "admin.recheckAction": { params: RecheckActionRequest; result: ActionView };
  "admin.resolveAction": { params: ResolveActionRequest; result: ActionView };
  "admin.adoptVersion": { params: AdoptVersionRequest; result: VersionPointer };
}
export type PublicMethod = keyof PublicRPCMap;
export type RPCRequest = {
  [M in PublicMethod]: {
    protocolVersion: "harness/1";
    requestID: string;
    binding: M extends "session.getInfo" ? SessionBinding | null : SessionBinding;
    method: M;
    params: PublicRPCMap[M]["params"];
  }
}[PublicMethod];
export type RPCResponse<M extends PublicMethod = PublicMethod> =
  | { protocolVersion: "harness/1"; requestID: string; binding: SessionBinding; ok: true; result: PublicRPCMap[M]["result"] }
  | { protocolVersion: "harness/1"; requestID: string | null; binding: SessionBinding | null; ok: false; error: ErrorInfo };
