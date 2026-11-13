// Private shared-contract entry. The root facade selects the public SDK exports.
export type {
  Json, Timestamp, CoreState, DefinitionKind, Ref, VersionSelector, Issue,
  Continuation, Outcome, CapacityResult, MetricSample, Report, CancellationInfo,
  RunLimits, CallOptions, PageRequest, Page, ErrorCode, ErrorInfo, SessionBinding,
  DomainInfo, ActionRequest, AppliedPolicy, PreparedActionRequest, Checkpoint,
  AttemptView, ActionObservation, ActionView, WaitView, RunView, SubmitRequest,
  SubmitReceipt, CancelRequest, VersionQuery, VersionPointer, CommandOperation,
  CommandView, HistorySubject, HistoryRequest, HistoryRecord, HarnessClient,
  Fact, Wake, AgentRunView, AdvanceInput, Decision, AgentDriver, CapacityContext,
  Reconciliation, CapacityDefinition, BindingContext, DependencyBinding,
  PolicyContext, HostPolicy, ProgressEvent, MetricEvent, NodeOptions,
  NodeStopResult, HarnessNode, ResumeRunRequest, RecheckActionRequest,
  ActionResolution, ResolveActionRequest, AdoptVersionRequest, HarnessAdmin,
  ConnectOptions, HarnessSession, ConnectHarness, PublicRPCMap, PublicMethod,
  RPCRequest, RPCResponse,
} from "./types.js";
export { HarnessError, harnessError, toHarnessError } from "./errors.js";
export { copyJson, isCopyJsonValidationError, parseJson, canonicalJson, jsonEqual, assertJsonValueSize } from "./json.js";
export { canonicalJsonChunks, canonicalJsonDigest } from "./json-chunks.js";
export { InternalCanonicalJsonDecoder } from "./json-stream.js";
export {
  validateRPCRequest, parseRPCRequest, validateRPCResponse, validateErrorInfo,
  validateAdvanceInput, validateDecision, validateReport,
} from "./validation.js";
export type {
  DefinitionDescriptor, ValidationPayload, ValidationAssignment, ValidationConclusion, DriverAssignment, FinalizeAssignment, StartAssignment, WakeAssignment, ActionAssignment, ActionReport, DriverConclusion, FinalizeConclusion, NodeAssignment, NodeConclusion, BusinessLeaseGrant,
  NodeSession, NodeIdentity, NodeRPCMap, NodeMethod, NodeCall, NodeRPCRequest, NodeRPCResponse,
} from "./node.js";
export { validateNodeRequest, validateNodeResponse, validateValidationPayload, validateValidationAssignment, validateActionRequest, validatePreparedActionRequest } from "./validation.js";

export { nodeStopSnapshot } from "./node.js";
export { compactPolicyPayload, compactReportPayload, expandDispatchPayload, compactDispatchPayload, dispatchPayloadMatches, dispatchOriginalMatches } from "./dispatch-binding.js";
