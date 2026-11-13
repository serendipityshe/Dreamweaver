export const nodeStopSnapshot = Symbol("nodeStopSnapshot");

// Coordinator/node-only protocol. Deliberately absent from the public facade.
import type { ActionRequest, PreparedActionRequest, Checkpoint, AdvanceInput, AppliedPolicy, CallOptions, DefinitionKind, ErrorInfo, Issue, Json, PublicMethod, Ref, SessionBinding } from "./types.js";

export interface DefinitionDescriptor { kind: DefinitionKind | "policy"; ref: Ref; supportsReconcile?: boolean }
export interface ValidationPayload {
  kind: DefinitionKind;
  ref: Ref;
  phase: "input" | "output" | "rewrite" | "admission" | "report-output";
  value: Json;
  config: Json;
  principalID: string;
  operation: PublicMethod;
  /** Complete original request/candidate/report, including its authorization context. */
  context: Json;
}
export interface ValidationAssignment {
  jobID: string;
  roundID: string;
  payload: ValidationPayload;
  assignmentID: string;
  assignmentEpoch: number;
  nodeID: string;
  incarnation: string;
  deadlineAt: number;
  roundDeadlineAt: number;
}
export interface ValidationConclusion { valid: boolean; issue: Issue | null; rewritten?: ActionRequest[] }
export interface BusinessLeaseGrant {
  kind: "run" | "action"; subjectID: string; nodeID: string; workerPrincipalID: string; incarnation: string;
  epoch: number; expiresAt: number; attemptID: string | null; attemptNumber: number | null;
}
export interface DriverAssignment {
  driver: true; jobID: string; runID: string; principalID: string; input: AdvanceInput;
  assignmentID: string; assignmentEpoch: number; nodeID: string; incarnation: string; deadlineAt: number;
  lease: BusinessLeaseGrant; leaseTTLms: number;
}
export interface FinalizeAssignment extends Omit<DriverAssignment, "driver"> { finalize: true }
export interface StartAssignment {
  start: true; jobID: string; runID: string; principalID: string; agent: Ref;
  assignmentID: string; assignmentEpoch: number; nodeID: string; incarnation: string; deadlineAt: number;
  lease: BusinessLeaseGrant; leaseTTLms: number;
}
export interface WakeAssignment extends Omit<StartAssignment, "start"> { wake: true; waitID: string }
export interface ActionAssignment {
  action: true; runID: string; rootID: string; actionID: string; principalID: string; request: PreparedActionRequest;
  checkpoint: Checkpoint; mode: "automatic-recovery" | "recheck-only"; recovering: boolean; stage: "execute" | "reconcile";
  attemptID: string; attemptNumber: number; assignmentID: string; nodeID: string; incarnation: string;
  /** Absent on a replay of a legacy assignment whose value budget was not recorded. */
  reportValueLimit?: number;
  lease: BusinessLeaseGrant; leaseTTLms: number;
}
export type ActionReport = { kind: "report"; value: Json } | { kind: "diagnostic"; code: string; phase: "execute" | "reconcile"; digest?: string };
export type DriverConclusion = { ok: true; decision: Json } | { ok: false; issue: Issue };
export type FinalizeConclusion = { ok: true; finalized: true };
export type NodeAssignment = ValidationAssignment | DriverAssignment | FinalizeAssignment | StartAssignment | WakeAssignment | ActionAssignment;
export type NodeConclusion = ValidationConclusion | DriverConclusion | FinalizeConclusion | { ok: true; started: true } | { ok: true; woken: true };
export interface NodeSession {
  nodeID: string;
  incarnation: string;
  expiresAt: number;
  serverTime: number;
  heartbeatMs: number;
  pollMs: number;
}
export interface NodeIdentity { nodeID: string; incarnation: string }
export interface NodeRPCMap {
  register: { params: NodeIdentity & { definitions: DefinitionDescriptor[]; concurrency: { runs: number; actions: number }; policies: AppliedPolicy[] }; result: NodeSession };
  renew: { params: NodeIdentity & { renewRequestID: string }; result: NodeSession };
  quiesce: { params: NodeIdentity & { pendingClaimRequestID?: string }; result: { quiesced: true } };
  claim: { params: NodeIdentity & { claimRequestID: string; validationAvailable?: boolean; runAvailable?: boolean; actionAvailable?: boolean }; result: { assignment: NodeAssignment | null } };
  report: { params: NodeIdentity & { assignment: NodeAssignment; conclusion: NodeConclusion; grant?: BusinessLeaseGrant }; result: { conclusion: NodeConclusion } };
  renewLease: { params: NodeIdentity & { grant: BusinessLeaseGrant; renewRequestID: string }; result: { grant: BusinessLeaseGrant; ttlMs: number } };
  verifyLease: { params: NodeIdentity & { grant: BusinessLeaseGrant }; result: { valid: true } };
  agentExecution: { params: NodeIdentity & { grant: BusinessLeaseGrant }; result: { runID: string; rootID: string; principalID: string; actionRunID: string | null; request: PreparedActionRequest | null } };
  verifyValidation: { params: NodeIdentity & { assignment: ValidationAssignment }; result: { valid: true } };
  readValidationValue: { params: NodeIdentity & { assignment: ValidationAssignment; offset: number; maxBytes: number; source?: "policy" | "report" | "action" | "run-config" }; result: { offset: number; data: string; done: boolean } };
  releaseLease: { params: NodeIdentity & { grant: BusinessLeaseGrant; releaseRequestID: string }; result: { released: true } };
  dispatchAction: { params: NodeIdentity & { grant: BusinessLeaseGrant; actionID: string; attemptID: string; dispatchRequestID: string }; result: { executionCount: number } };
  actionContinuation: { params: NodeIdentity & { grant: BusinessLeaseGrant; actionID: string; attemptID: string }; result: { state: "pending" | "execute" | "stop" } };
  checkpointAction: { params: NodeIdentity & { grant: BusinessLeaseGrant; actionID: string; attemptID: string; operationID: string; expectedRevision: number; value: Json }; result: { checkpoint: Checkpoint } };
  readCheckpoint: { params: NodeIdentity & { grant: BusinessLeaseGrant; actionID: string; attemptID: string; operationID: string }; result: { checkpoint: Checkpoint | null } };
  reportAction: { params: NodeIdentity & { grant: BusinessLeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: "execute" | "reconcile"; report: ActionReport; observationOnly?: boolean }; result: { stored: true } };
  diagnoseAction: { params: NodeIdentity & { grant: BusinessLeaseGrant; actionID: string; attemptID: string; phase: "execute" | "reconcile"; diagnosticID: string; code: string }; result: { stored: true } };
  unregister: { params: NodeIdentity; result: { stopped: true } };
}
export type NodeMethod = keyof NodeRPCMap;
export type NodeCall = <M extends NodeMethod>(method: M, params: NodeRPCMap[M]["params"], options?: CallOptions) => Promise<NodeRPCMap[M]["result"]>;
export type NodeRPCRequest = { [M in NodeMethod]: {
  protocolVersion: "harness/1"; requestID: string; binding: SessionBinding; method: M; params: NodeRPCMap[M]["params"];
} }[NodeMethod];
export type NodeRPCResponse<M extends NodeMethod = NodeMethod> =
  | { protocolVersion: "harness/1"; requestID: string; binding: SessionBinding; ok: true; result: NodeRPCMap[M]["result"] }
  | { protocolVersion: "harness/1"; requestID: string | null; binding: SessionBinding | null; ok: false; error: ErrorInfo };
