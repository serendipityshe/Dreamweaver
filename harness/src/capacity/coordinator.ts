import { verifySubmitInput } from "../persistence/index.js";
import { runConfigValidationSource } from "../persistence/index.js";
import { stageEpochValue, abandonEpochValue, readActionReport, verifyInlineReportReceipt } from "../persistence/index.js";
import { stageValidationValue, abandonValidationValue, validationPublicationWrites, validationValueSource, readValidationConclusionAsync, type ValidationValueSource } from "../persistence/index.js";
import { stageNodePolicies, abandonNodePolicies, policyPublicationWrites, encodeNodeConfiguration, readNodeConfiguration, samePolicyValue, storedPolicySource, verifyPolicySource, policyPayloadSource } from "../persistence/index.js";
import { verifyCommandValue, commandRequestMetadata, type CommandValueSource } from "../persistence/index.js";
import { encodeCheckpoint, readCheckpointValueAsync } from "../persistence/index.js";
import { hasChildFrameSource } from "../persistence/index.js";
import { readFixedActionRequest, readFixedActionRequestAsync, fixedActionRequestMatches, actionRequestValueSource } from "../persistence/index.js";
import { validationReportRequest, expandActionRequests } from "../persistence/index.js";
import { driverValueSource, verifyDriverValueSource, stageDriverValue, readRunSnapshot } from "../persistence/index.js";
import { randomUUID } from "node:crypto";
import {
  assertJsonValueSize, canonicalJson, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, HarnessError, parseJson, validateValidationPayload, validateAdvanceInput, validateActionRequest, validatePreparedActionRequest,
  type DefinitionDescriptor, type Json, type NodeRPCMap, type NodeRPCRequest, type NodeSession,
  type ValidationAssignment, type ValidationPayload, type ValidationConclusion, type DriverAssignment, type FinalizeAssignment, type StartAssignment, type WakeAssignment, type ActionAssignment, type ActionReport, type DriverConclusion, type NodeAssignment, type BusinessLeaseGrant, type PreparedActionRequest, type Checkpoint, type RunView, type ActionView,
} from "../contract/index.js";
import { SQLiteStore, ValueStore, actionReportValueReference, frameJobInput, readFrameInput, makeFrameAssignmentReference, readFrameAssignment, sameFrameAssignment, readActionControl, reportValueOwner, type ValuePublication, type VerifiedReport, type SQLWrite, type HistoryEvent, type CommitBatch } from "../persistence/index.js";

interface LeaseService {
  prepareClaim(request: { kind: "run" | "action"; subjectID: string; nodeID: string; incarnation: string; workerPrincipalID: string; claimRequestID: string }, recheck?: boolean): { grant: BusinessLeaseGrant | null; writes: SQLWrite[]; validBefore: number };
  assert(grant: BusinessLeaseGrant): void;
  guard(grant: BusinessLeaseGrant): SQLWrite[];
  renew(request: BusinessLeaseGrant & { renewRequestID: string }): BusinessLeaseGrant;
  release(request: BusinessLeaseGrant & { releaseRequestID: string }): void;
}
interface ActionService {
  prepareClaim(grant: BusinessLeaseGrant): { runID: string; rootID: string; principalID: string; request: PreparedActionRequest; checkpoint: Checkpoint; mode: "automatic-recovery" | "recheck-only"; recovering: boolean; stage: "execute" | "reconcile"; attemptID: string; attemptNumber: number; reportValueLimit: number; writes: SQLWrite[]; events: HistoryEvent[] };
  dispatch(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; dispatchRequestID: string }): { executionCount: number };
  continuation(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string }): { state: "pending" | "execute" | "stop" };
  checkpoint(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; operationID: string; expectedRevision: number; value: Json }, assertCaller?: () => void): Checkpoint | Promise<Checkpoint>;
  checkpointReplay(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; operationID: string; expectedRevision: number; value: Json }): Checkpoint | null;
  readCheckpoint(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; operationID: string }): Checkpoint | null;
  readCheckpointReceipt(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; operationID: string }): Json | null;
  recordReport(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: "execute" | "reconcile"; report: ActionReport }, businessAuthorized?: boolean): { stored: true };
  prepareReportTransferSource(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: "execute" | "reconcile" }): SQLWrite[];
  authenticateReportTransferSource(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: "execute" | "reconcile" }): void;
  preparePublishedReportTransferSource(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: "execute" | "reconcile" }): SQLWrite[];
  recordReportValue(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: "execute" | "reconcile"; report: { kind: "report"; value: Json } }, ref: ValuePublication, reauthorize: () => boolean, transfer?: { id: string; sourceDigest: string; workerDigest: string }, currentGrant?: () => BusinessLeaseGrant): Promise<{ stored: true } | { stored: false; diagnostic: true }>;
  recordVerifiedReportValue(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; assignmentID: string; phase: "execute" | "reconcile" }, ref: ValuePublication, proof: VerifiedReport, reauthorize: () => boolean, transfer?: { id: string; sourceDigest: string; workerDigest: string }): Promise<{ stored: true } | { stored: false; diagnostic: true }>;
  recordDiagnostic(input: { grant: BusinessLeaseGrant; actionID: string; attemptID: string; phase: "execute" | "reconcile"; diagnosticID: string; code: string }, businessAuthorized?: boolean): { stored: true };
}

export interface NodeServiceOptions {
  clock?: { now(): number };
  sessionTTLms?: number;
  assignmentTTLms?: number;
  heartbeatMs?: number;
  pollMs?: number;
}
export interface ValidationJobInput { jobID: string; payload: ValidationPayload }
export interface PreparationInput {
  roundID: string;
  ownerKind: "command" | "frame" | "report";
  ownerID: string;
  principalID: string;
  phase: string;
  timeoutMs: number;
  jobs: ValidationJobInput[];
}
const write = (sql: string, params: SQLWrite["params"], expectChanges = 1): SQLWrite => ({ sql, params, expectChanges });
const json = (value: unknown): Json => copyJson(value) as Json;
const id = (value: unknown): value is string => typeof value === "string" && value.length > 0 && value.trim() === value;

/** Pure node/validation use cases. Each atomic set is constructed and committed here. No business Run/Action writes. */
export class NodeService {
  private readonly checkpointResponses = new WeakMap<object, { request: object; receiptDigest: string }>();
  readonly options: Required<NodeServiceOptions>;
  constructor(readonly store: SQLiteStore, options: NodeServiceOptions = {}, readonly authorizePayload: (payload: ValidationPayload) => void = () => {}, readonly maxValueBytes = 262_144, readonly canDrive: (runID: string) => boolean = () => true, readonly leases?: LeaseService, readonly actions?: ActionService, readonly canWake: (runID: string) => boolean = () => true, readonly valueQuota = Number.MAX_SAFE_INTEGER) {
    this.options = nodeServiceOptions(options);
    if (!Number.isSafeInteger(maxValueBytes) || maxValueBytes <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid node maxValueBytes");
  }

  getDefinitions(nodeID: string, incarnation: string): DefinitionDescriptor[] {
    return this.store.query(`SELECT d.kind,d.id,d.version FROM node_definitions d JOIN node_sessions s ON s.incarnation=d.incarnation
      WHERE s.node_id=? AND s.incarnation=? ORDER BY d.kind,d.id,d.version`, [nodeID, incarnation])
      .map(row => ({ kind: row.kind as DefinitionDescriptor["kind"], ref: { id: row.id as string, version: row.version as string } }));
  }

  /** Revalidate a frozen claim response without executing or replaying the claim. */
  async authorizeClaimFragment(params: NodeRPCMap["claim"]["params"], result: unknown, principalID: string, assertWorker: () => void): Promise<void> {
    this.store.assertAuthority();
    const now = this.now();
    this.session(params, principalID, now);
    if (this.store.query("SELECT enabled FROM node_claim_gates WHERE incarnation=?", [params.incarnation])[0]?.enabled !== 1)
      throw harnessError("NODE_STATE_INVALID", "Node has stopped claiming work");
    assertWorker();
    const operationID = identity("node.claim", [principalID, params.incarnation, params.claimRequestID]);
    const receipt = this.store.query("SELECT type,subject_id,request_json,result_json FROM history WHERE operation_id=?", [operationID])[0];
    if (!receipt || receipt.type !== "validation.claimed" || receipt.subject_id !== params.nodeID
      || receipt.request_json !== canonicalJson({ principalID, ...params }))
      throw harnessError("EXECUTION_FENCED", "Claim response no longer matches its original receipt");
    const original = await this.readClaimResult(parseJson(receipt.result_json as string));
    if (!await sameFrameAssignment(original, result)) throw harnessError("EXECUTION_FENCED", "Claim response no longer matches its original receipt");
    const assignment = (result as NodeRPCMap["claim"]["result"]).assignment;
    if (assignment) {
      if (assignment.nodeID !== params.nodeID || assignment.incarnation !== params.incarnation)
        throw harnessError("FORBIDDEN", "Claim assignment belongs to a different node session");
      await this.checkReplayedAssignment(assignment, now);
    }
    this.session(params, principalID, this.now());
    if (this.store.query("SELECT enabled FROM node_claim_gates WHERE incarnation=?", [params.incarnation])[0]?.enabled !== 1)
      throw harnessError("NODE_STATE_INVALID", "Node has stopped claiming work");
    assertWorker();
    if (assignment) this.checkReplayedAssignmentCurrent(assignment, this.now());
    this.store.assertAuthority();
  }

  /** Synchronous final eligibility check after asynchronous frame evidence
   * reads and immediately before bytes are made visible to the worker. */
  authorizeClaimFragmentCurrent(params: NodeRPCMap["claim"]["params"], result: unknown, principalID: string, assertWorker: () => void): void {
    this.store.assertAuthority(); this.session(params, principalID, this.now());
    if (this.store.query("SELECT enabled FROM node_claim_gates WHERE incarnation=?", [params.incarnation])[0]?.enabled !== 1)
      throw harnessError("NODE_STATE_INVALID", "Node has stopped claiming work");
    assertWorker();
    const assignment = (result as NodeRPCMap["claim"]["result"]).assignment;
    if (assignment) {
      if (assignment.nodeID !== params.nodeID || assignment.incarnation !== params.incarnation) throw harnessError("FORBIDDEN", "Claim assignment belongs to a different node session");
      this.checkReplayedAssignmentCurrent(assignment, this.now());
    }
    this.store.assertAuthority();
  }

  /** Verify checkpoint response provenance and current eligibility without saving again. */
  authorizeCheckpointFragment(request: Extract<NodeRPCRequest, { method: "checkpointAction" | "readCheckpoint" }>, result: unknown, principalID: string, assertWorker: () => void): void {
    this.store.assertAuthority();
    const p = request.params;
    if (request.method === "checkpointAction") this.session(p, principalID, this.now());
    assertWorker();
    if (!this.actions || !this.leases) throw harnessError("STATE_CONFLICT", "Action execution is unavailable");
    if (p.grant.subjectID !== p.actionID || p.grant.attemptID !== p.attemptID) throw harnessError("FORBIDDEN", "Action attempt identity mismatch");
    this.checkActionIdentity(p.grant, p, principalID);
    if (request.method === "checkpointAction") {
      this.checkLeaseIdentity(p.grant, p, principalID);
      this.leases.assert(p.grant);
    }
    const source = { grant: p.grant, actionID: p.actionID, attemptID: p.attemptID, operationID: p.operationID };
    const encoded = this.actions.readCheckpointReceipt(source), receiptDigest = canonicalJsonDigest(encoded);
    const prior = result && typeof result === "object" ? this.checkpointResponses.get(result) : undefined;
    if (prior?.request === request && prior.receiptDigest === receiptDigest) { this.store.assertAuthority(); return; }
    const checkpoint = this.actions.readCheckpoint(source);
    const same = (left: unknown, right: unknown) => {
      const a = canonicalJsonChunks(left)[Symbol.iterator](), b = canonicalJsonChunks(right)[Symbol.iterator]();
      for (;;) { const x = a.next(), y = b.next(); if (x.done || y.done) return x.done === y.done; if (!Buffer.from(x.value).equals(y.value)) return false; }
    };
    if (!same(result, { checkpoint })) throw harnessError("EXECUTION_FENCED", "Checkpoint response no longer matches its source");
    if (request.method === "checkpointAction") {
      const original = this.actions.checkpointReplay({ ...source, expectedRevision: request.params.expectedRevision, value: request.params.value });
      if (!original || !same(original, checkpoint)) throw harnessError("EXECUTION_FENCED", "Checkpoint response does not match its original save");
    }
    if (result && typeof result === "object") this.checkpointResponses.set(result, { request, receiptDigest });
    this.store.assertAuthority();
  }

  handle(request: NodeRPCRequest, workerPrincipalID: string, assertWorker: () => void = () => {}): NodeRPCMap[keyof NodeRPCMap]["result"] | Promise<NodeRPCMap[keyof NodeRPCMap]["result"]> {
    this.store.assertAuthority();
    switch (request.method) {
      case "register": return this.register(request.params, workerPrincipalID, assertWorker);
      case "renew": return this.renew(request.params, workerPrincipalID);
      case "quiesce": return this.quiesce(request.params, workerPrincipalID);
      case "claim": return this.claim(request.params, workerPrincipalID, assertWorker);
      case "report": return this.report(request.params, workerPrincipalID, assertWorker);
      case "renewLease": return this.renewLease(request.params, workerPrincipalID);
      case "verifyLease": return this.verifyLease(request.params, workerPrincipalID);
      case "agentExecution": return this.agentExecution(request.params, workerPrincipalID);
      case "verifyValidation": return this.verifyValidation(request.params, workerPrincipalID);
      case "readValidationValue": return this.readValidationValue(request.params, workerPrincipalID, assertWorker);
      case "releaseLease": return this.releaseLease(request.params, workerPrincipalID);
      case "dispatchAction": return this.actionCall(request.method, request.params, workerPrincipalID);
      case "actionContinuation": return this.actionCall(request.method, request.params, workerPrincipalID);
      case "checkpointAction": return this.actionCall(request.method, request.params, workerPrincipalID, assertWorker);
      case "readCheckpoint": return this.actionCall(request.method, request.params, workerPrincipalID);
      case "reportAction": return this.actionCall(request.method, request.params, workerPrincipalID, assertWorker);
      case "diagnoseAction": return this.actionCall(request.method, request.params, workerPrincipalID);
      case "unregister": return this.unregister(request.params, workerPrincipalID);
    }
  }

  private checkLeaseIdentity(grant: BusinessLeaseGrant, params: { nodeID: string; incarnation: string }, principalID: string): void {
    if (grant.nodeID !== params.nodeID || grant.incarnation !== params.incarnation || grant.workerPrincipalID !== principalID)
      throw harnessError("FORBIDDEN", "Lease is not associated with this worker");
    if (grant.kind === "action") {
      this.checkActionIdentity(grant, params, principalID);
      const current = this.store.query(`SELECT s.attempt_id FROM action_attempt_state s JOIN work_items w ON w.kind='action' AND w.subject_id=s.action_id AND w.stage IN ('running','report_pending')
        WHERE s.action_id=? AND s.attempt_id=?`, [grant.subjectID, grant.attemptID])[0];
      if (!current) throw harnessError("EXECUTION_FENCED", "Action attempt work is no longer active");
      return;
    }
    const rows = this.store.query(`SELECT j.principal_id,j.agent_id,j.agent_version,'active' AS phase FROM driver_jobs j
      JOIN frames f ON f.id=j.id AND f.run_id=j.run_id AND f.state='open'
      JOIN work_items w ON w.kind='run' AND w.subject_id=j.run_id AND w.stage='driver'
      WHERE j.run_id=? AND j.node_incarnation=? AND j.lease_epoch=? AND j.coordinator_epoch=? AND j.state IN ('assigned','accepted')
      UNION ALL SELECT j.principal_id,j.agent_id,j.agent_version,'active' AS phase FROM finalize_jobs j
      JOIN frames f ON f.id=j.id AND f.run_id=j.run_id AND f.state='open'
      JOIN work_items w ON w.kind='run' AND w.subject_id=j.run_id AND w.stage IN ('driver','finalize')
      WHERE j.run_id=? AND j.node_incarnation=? AND j.lease_epoch=? AND j.coordinator_epoch=? AND j.state IN ('assigned','accepted')
      UNION ALL SELECT j.principal_id,j.agent_id,j.agent_version,'start' AS phase FROM start_jobs j
      JOIN work_items w ON w.kind='run' AND w.subject_id=j.id AND w.stage='start'
      JOIN runs r ON r.id=j.id AND r.core_json IS NULL
      WHERE j.id=? AND j.node_incarnation=? AND j.lease_epoch=? AND j.coordinator_epoch=? AND j.state IN ('assigned','accepted')
      UNION ALL SELECT j.principal_id,j.agent_id,j.agent_version,'wake' AS phase FROM wake_jobs j
      JOIN waits t ON t.id=j.id AND t.run_id=j.run_id AND t.winner IS NOT NULL AND t.wake_json IS NOT NULL
      JOIN work_items w ON w.kind='run' AND w.subject_id=j.run_id AND w.stage='wake'
      WHERE j.run_id=? AND j.node_incarnation=? AND j.lease_epoch=? AND j.state IN ('assigned','accepted') LIMIT 1`,
      [grant.subjectID, grant.incarnation, grant.epoch, this.store.coordinatorEpoch, grant.subjectID, grant.incarnation, grant.epoch, this.store.coordinatorEpoch, grant.subjectID, grant.incarnation, grant.epoch, this.store.coordinatorEpoch, grant.subjectID, grant.incarnation, grant.epoch]);
    const row = rows[0];
    if (!row) throw harnessError("EXECUTION_FENCED", "Lease has no associated current frame work");
    if (row.phase === "active" && !this.canDrive(grant.subjectID)) throw harnessError("EXECUTION_FENCED", "Run Core is not available");
    if (row.phase === "wake" && !this.canWake(grant.subjectID)) throw harnessError("EXECUTION_FENCED", "Suspended Run Core is not available");
    this.authorizePayload({ kind: "agent", ref: { id: row.agent_id as string, version: row.agent_version as string }, phase: "input", value: null,
      config: null, principalID: row.principal_id as string, operation: "client.submit", context: null });
  }
  private checkActionIdentity(grant: BusinessLeaseGrant, params: { nodeID: string; incarnation: string }, principalID: string, allowSourceOnly = false): boolean {
    if (grant.kind !== "action" || grant.nodeID !== params.nodeID || grant.incarnation !== params.incarnation || grant.workerPrincipalID !== principalID || !grant.attemptID)
      throw harnessError("FORBIDDEN", "Action lease is not associated with this worker");
    const owner = this.store.query("SELECT principal_id FROM node_sessions WHERE incarnation=? AND node_id=?", [grant.incarnation, grant.nodeID])[0];
    const action = this.store.query(`SELECT t.capacity_id,t.capacity_version,r.principal_id FROM action_auth_targets t
      JOIN actions a ON a.id=t.action_id JOIN runs r ON r.id=a.run_id WHERE t.action_id=?`, [grant.subjectID])[0];
    if (!owner || owner.principal_id !== principalID || !action) throw harnessError("FORBIDDEN", "Action source is not authorized");
    const target = { id: action.capacity_id as string, version: action.capacity_version as string };
    let businessAuthorized = true;
    try { this.authorizePayload({ kind: "capacity", ref: target, phase: "input", value: null, config: null,
      principalID: action.principal_id as string, operation: "client.submit", context: null }); }
    catch (error) {
      if (!allowSourceOnly || !error || typeof error !== "object" || !("info" in error) || (error.info as { code?: string }).code !== "FORBIDDEN") throw error;
      businessAuthorized = false;
    }
    const installed = this.getDefinitions(params.nodeID, params.incarnation);
    const policies = this.store.query("SELECT policy_id,policy_version FROM action_auth_policies WHERE action_id=? ORDER BY ordinal", [grant.subjectID]);
    if (!installed.some(item => item.kind === "capacity" && item.ref.id === target.id && item.ref.version === target.version)
        || policies.some(policy => !installed.some(item => item.kind === "policy" && item.ref.id === policy.policy_id && item.ref.version === policy.policy_version)))
      throw harnessError("FORBIDDEN", "Fixed Action definitions are not available to this worker");
    return businessAuthorized;
  }
  /** Authenticate the immutable source before staging and again on each
   * fragment. Returned CAS guards join source phase to the write transaction. */
  prepareReportTransferSource(params: Omit<NodeRPCMap["reportAction"]["params"], "report">, principalID: string, assertWorker: () => void): { owner: string; businessAuthorized: boolean; guards: SQLWrite[] } {
    this.store.assertAuthority();
    assertWorker();
    if (!this.actions || params.grant.subjectID !== params.actionID || params.grant.attemptID !== params.attemptID)
      throw harnessError("FORBIDDEN", "Action report source is unavailable");
    const businessAuthorized = this.checkActionIdentity(params.grant, params, principalID, true);
    const guards = this.actions.prepareReportTransferSource({ grant: params.grant, actionID: params.actionID, attemptID: params.attemptID, assignmentID: params.assignmentID, phase: params.phase });
    this.store.assertAuthority();
    return { owner: reportValueOwner(this.store.domainID, params.actionID, params.attemptID, params.phase), businessAuthorized: businessAuthorized && params.observationOnly !== true, guards };
  }
  /** Reauthenticate an already published transfer without restoring a lease. */
  preparePublishedReportTransferSource(params: Omit<NodeRPCMap["reportAction"]["params"], "report">, principalID: string, assertWorker: () => void): { owner: string; businessAuthorized: boolean; guards: SQLWrite[] } {
    this.store.assertAuthority(); assertWorker();
    if (!this.actions || params.grant.subjectID !== params.actionID || params.grant.attemptID !== params.attemptID)
      throw harnessError("FORBIDDEN", "Action report source is unavailable");
    const businessAuthorized = this.checkActionIdentity(params.grant, params, principalID, true);
    const guards = this.actions.preparePublishedReportTransferSource(params);
    this.store.assertAuthority();
    return { owner: reportValueOwner(this.store.domainID, params.actionID, params.attemptID, params.phase), businessAuthorized: businessAuthorized && params.observationOnly !== true, guards };
  }
  /** Complete an authenticated staging transfer; not exposed by node RPC. */
  recordStagedReport(params: NodeRPCMap["reportAction"]["params"], ref: ValuePublication, proof: VerifiedReport, transfer: { id: string; sourceDigest: string; workerDigest: string }, principalID: string, assertWorker: () => void): Promise<{ stored: true } | { stored: false; diagnostic: true }> {
    params = copyJson(params) as unknown as typeof params;
    if (!this.actions || params.report.kind !== "report") throw harnessError("INVALID_ARGUMENT", "Staged content must be a report");
    const actions = this.actions;
    const authorize = () => {
      this.store.assertAuthority(); assertWorker();
      if (params.grant.subjectID !== params.actionID || params.grant.attemptID !== params.attemptID) throw harnessError("FORBIDDEN", "Action report source is unavailable");
      const businessAuthorized = this.checkActionIdentity(params.grant, params, principalID, true);
      actions.authenticateReportTransferSource(params);
      return businessAuthorized && params.observationOnly !== true;
    };
    authorize();
    return actions.recordVerifiedReportValue({ grant: params.grant, actionID: params.actionID, attemptID: params.attemptID,
      assignmentID: params.assignmentID, phase: params.phase }, ref, proof, authorize, transfer);
  }
  private actionCall(method: "dispatchAction" | "actionContinuation" | "checkpointAction" | "readCheckpoint" | "reportAction" | "diagnoseAction",
    params: NodeRPCMap["dispatchAction" | "actionContinuation" | "checkpointAction" | "readCheckpoint" | "reportAction" | "diagnoseAction"]["params"], principalID: string, assertWorker: () => void = () => {}): NodeRPCMap["dispatchAction" | "actionContinuation" | "checkpointAction" | "readCheckpoint" | "reportAction" | "diagnoseAction"]["result"] | Promise<NodeRPCMap["checkpointAction" | "reportAction"]["result"]> {
    if (!this.actions || !this.leases) throw harnessError("STATE_CONFLICT", "Action execution is unavailable");
    if (params.grant.subjectID !== params.actionID || params.grant.attemptID !== params.attemptID) throw harnessError("FORBIDDEN", "Action attempt identity mismatch");
    const businessAuthorized = this.checkActionIdentity(params.grant, params, principalID, method === "reportAction" || method === "diagnoseAction");
    if (method === "actionContinuation") {
      this.leases.assert(params.grant);
      return this.actions.continuation({ grant: params.grant, actionID: params.actionID, attemptID: params.attemptID });
    }
    if (method === "dispatchAction") {
      this.leases.assert(params.grant);
      const p = params as NodeRPCMap["dispatchAction"]["params"];
      return this.actions.dispatch({ grant: p.grant, actionID: p.actionID, attemptID: p.attemptID, dispatchRequestID: p.dispatchRequestID });
    }
    if (method === "checkpointAction") {
      this.leases.assert(params.grant);
      const p = params as NodeRPCMap["checkpointAction"]["params"];
      const check = () => { assertWorker(); this.session(p, principalID, this.now()); this.checkActionIdentity(p.grant, p, principalID); this.leases!.assert(p.grant); };
      const checkpoint = this.actions.checkpoint({ grant: p.grant, actionID: p.actionID, attemptID: p.attemptID, operationID: p.operationID, expectedRevision: p.expectedRevision, value: p.value }, check);
      return checkpoint instanceof Promise ? checkpoint.then(value => ({ checkpoint: value })) : { checkpoint };
    }
    if (method === "readCheckpoint") {
      const p = params as NodeRPCMap["readCheckpoint"]["params"];
      return { checkpoint: this.actions.readCheckpoint({ grant: p.grant, actionID: p.actionID, attemptID: p.attemptID, operationID: p.operationID }) };
    }
    if (method === "reportAction") {
      const p = params as NodeRPCMap["reportAction"]["params"];
      if (p.report.kind === "report") {
        let bytes = 0; for (const part of canonicalJsonChunks(p.report)) { bytes += part.length; if (bytes > 16_384) return this.reportActionValue(p, principalID, assertWorker); }
      }
      return this.actions.recordReport({ grant: p.grant, actionID: p.actionID, attemptID: p.attemptID, assignmentID: p.assignmentID, phase: p.phase, report: p.report }, businessAuthorized && p.observationOnly !== true);
    }
    const p = params as NodeRPCMap["diagnoseAction"]["params"];
    return this.actions.recordDiagnostic({ grant: p.grant, actionID: p.actionID, attemptID: p.attemptID, phase: p.phase, diagnosticID: p.diagnosticID, code: p.code }, businessAuthorized);
  }
  /** Ordinary production reports share the existing value/receipt pipeline;
   * this does not enable v4 or require a report spool on the worker. */
  private async reportActionValue(params: NodeRPCMap["reportAction"]["params"], principalID: string, assertWorker: () => void): Promise<{ stored: true }> {
    const actions = this.actions!;
    if (params.report.kind !== "report") throw harnessError("INVALID_ARGUMENT", "Expected an Action report");
    const input = { grant: params.grant, actionID: params.actionID, attemptID: params.attemptID, assignmentID: params.assignmentID, phase: params.phase, report: params.report };
    // Staging can outlast the supplied deadline while the worker keeps renewing
    // this exact lease. Only a grant valid at admission may follow those durable
    // renewals; an already late or forged grant must stay observational.
    let followRenewals = params.observationOnly !== true;
    try { if (followRenewals) this.leases!.assert(params.grant); }
    catch (error) { if (!(error instanceof HarnessError) || error.info.code !== "EXECUTION_FENCED") throw error; followRenewals = false; }
    const currentGrant = () => {
      if (!followRenewals) return params.grant;
      const row = this.store.query("SELECT expires_at FROM business_leases WHERE kind='action' AND subject_id=?", [params.actionID])[0];
      if (!row) { followRenewals = false; return params.grant; }
      const renewed = { ...params.grant, expiresAt: row.expires_at as number };
      try { this.leases!.assert(renewed); return renewed; }
      catch (error) {
        if (!(error instanceof HarnessError) || error.info.code !== "EXECUTION_FENCED") throw error;
        followRenewals = false; return params.grant;
      }
    };
    const check = () => {
      this.store.assertAuthority(); assertWorker();
      const businessAuthorized = this.checkActionIdentity(params.grant, params, principalID, true);
      currentGrant();
      actions.authenticateReportTransferSource(input); return businessAuthorized && params.observationOnly !== true;
    };
    const find = () => this.store.query("SELECT * FROM action_reports WHERE action_id=? AND attempt_id=? AND phase=?", [params.actionID, params.attemptID, params.phase])[0];
    const conflict = () => harnessError("REQUEST_CONFLICT", "Action report identity is bound to different content");
    const usePublished = async (ref: ValuePublication) => {
      try { await actions.recordReportValue(input, ref, check, undefined, currentGrant); return { stored: true as const }; }
      catch (error) { if (error instanceof HarnessError && error.info.code === "INVALID_ARGUMENT") throw conflict(); throw error; }
    };
    check(); const previous = find(), previousRef = previous ? actionReportValueReference(this.store, previous) : null;
    if (previousRef) return usePublished(previousRef);
    if (previous && (parseJson(previous.report_json as string) as Record<string, Json>).kind === "report") {
      // Old inline receipts remain authoritative; compare complete canonical
      // bytes without rewriting them or registering a second source.
      verifyInlineReportReceipt(this.store, previous);
      const old = await readActionReport(this.store, previous), a = canonicalJsonChunks(old)[Symbol.iterator](), b = canonicalJsonChunks(params.report)[Symbol.iterator]();
      for (;;) { check(); const x = a.next(), y = b.next(); if (x.done || y.done) { if (x.done !== y.done) throw conflict(); break; } if (!Buffer.from(x.value).equals(y.value)) throw conflict(); await new Promise<void>(resolve => setImmediate(resolve)); }
      return { stored: true };
    }
    let staged: ValuePublication | null = null;
    try {
      staged = await stageEpochValue(this.store, params.report, reportValueOwner(this.store.domainID, params.actionID, params.attemptID, params.phase), "node-report", this.valueQuota, () => { check(); }, true);
      if (!staged) throw harnessError("INTERNAL_ERROR", "Report staging is unavailable");
      try { await actions.recordReportValue(input, staged, check, undefined, currentGrant); return { stored: true }; }
      catch (error) {
        // Concurrent same-source reporters may have sealed different temporary
        // IDs. Replay the winner only after exact comparison to its bytes.
        if (error instanceof HarnessError && error.info.code === "REQUEST_CONFLICT") { const winner = find(), ref = winner ? actionReportValueReference(this.store, winner) : null; if (ref && ref.id !== staged.id) return usePublished(ref); }
        throw error;
      }
    } finally { abandonEpochValue(this.store, staged); }
  }
  private renewLease(params: NodeRPCMap["renewLease"]["params"], principalID: string): NodeRPCMap["renewLease"]["result"] {
    if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable");
    this.checkLeaseIdentity(params.grant, params, principalID);
    const grant = this.leases.renew({ ...params.grant, renewRequestID: params.renewRequestID });
    const ttlMs = grant.expiresAt - this.now();
    if (ttlMs <= 0) throw harnessError("EXECUTION_FENCED", "Renewed lease expired");
    return { grant, ttlMs };
  }
  private verifyLease(params: NodeRPCMap["verifyLease"]["params"], principalID: string): NodeRPCMap["verifyLease"]["result"] {
    if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable");
    this.checkLeaseIdentity(params.grant, params, principalID);
    this.leases.assert(params.grant);
    return { valid: true };
  }
  /** Read the immutable parent Action under the current Run execution identity.
   * This is node-only metadata, never a worker-supplied authorization context. */
  private agentExecution(params: NodeRPCMap["agentExecution"]["params"], principalID: string): NodeRPCMap["agentExecution"]["result"] {
    if (!this.leases || params.grant.kind !== "run") throw harnessError("FORBIDDEN", "Agent execution requires a Run lease");
    this.checkLeaseIdentity(params.grant, params, principalID);
    this.leases.assert(params.grant);
    const row = this.store.query("SELECT id,principal_id,parent_action_id,snapshot_json FROM runs WHERE id=?", [params.grant.subjectID])[0];
    if (!row?.snapshot_json) throw harnessError("FORBIDDEN", "Run execution context is unavailable");
    const run = readRunSnapshot(this.store, row.snapshot_json);
    if (run.id !== row.id || run.parentActionID !== row.parent_action_id) throw harnessError("FORBIDDEN", "Run execution context is inconsistent");
    let request: PreparedActionRequest | null = null;
    let actionRunID: string | null = null;
    if (run.parentActionID !== null) {
      const action = this.store.query("SELECT a.id,a.run_id,a.request_json,a.snapshot_json,r.principal_id,r.snapshot_json parent_json FROM actions a JOIN runs r ON r.id=a.run_id WHERE a.id=?", [run.parentActionID])[0];
      if (!action?.snapshot_json || action.principal_id !== row.principal_id) throw harnessError("FORBIDDEN", "Agent Action source is unavailable");
      actionRunID = action.run_id as string;
      request = readFixedActionRequest(this.store, action.id as string, action.request_json);
      const view = expandActionRequests(this.store, parseJson(action.snapshot_json as string)) as { remoteRunID?: Json; request?: Json };
      const parent = parseJson(action.parent_json as string) as { rootID?: Json };
      const link = this.store.query("SELECT action_id FROM remote_run_links WHERE action_id=? AND child_run_id=?", [run.parentActionID, run.id])[0];
      if (!link || view.remoteRunID !== run.id || canonicalJson(view.request) !== canonicalJson(request) || parent.rootID !== run.rootID
          || request.kind !== "agent" || canonicalJson(request.target) !== canonicalJson(run.agent) || canonicalJson(request.input) !== canonicalJson(run.input)
          || canonicalJson(request.config) !== canonicalJson(run.config)) throw harnessError("FORBIDDEN", "Agent Action does not match its child Run");
    } else if (run.rootID !== run.id) throw harnessError("FORBIDDEN", "Root Run identity is inconsistent");
    return { runID: run.id, rootID: run.rootID, principalID: row.principal_id as string, actionRunID, request };
  }
  private verifyValidation(params: NodeRPCMap["verifyValidation"]["params"], principalID: string): NodeRPCMap["verifyValidation"]["result"] {
    if (params.assignment.nodeID !== params.nodeID || params.assignment.incarnation !== params.incarnation) throw harnessError("FORBIDDEN", "Validation assignment is not associated with this worker");
    this.session(params, principalID, this.now());
    this.authorizePayload(params.assignment.payload);
    if (!this.supportsPreparation(params.incarnation, params.assignment.payload)) throw harnessError("EXECUTION_FENCED", "Fixed policies are unavailable on this node");
    this.currentAssignment(params.assignment, this.now());
    return { valid: true };
  }
  /** The assignment and stored report are reauthorized for every bounded read. */
  private readValidationValue(params: NodeRPCMap["readValidationValue"]["params"], principalID: string, assertWorker: () => void): NodeRPCMap["readValidationValue"]["result"] {
    this.store.assertAuthority();
    assertWorker();
    this.verifyValidation(params, principalID);
    const payload = params.assignment.payload;
    const context = payload.context as Record<string, any> | null;
    if (params.source === "run-config") {
      const source = context?.runConfigSource;
      const round = this.store.query("SELECT owner_kind,owner_id,phase,principal_id FROM prepare_rounds WHERE id=?", [params.assignment.roundID])[0];
      if (payload.kind !== "agent" || payload.phase !== "output" || payload.operation !== "client.submit" || payload.config !== null
          || !source || source.encoding !== "run-config-source-v1" || typeof context?.runID !== "string" || typeof context.frameID !== "string"
          || !round || round.owner_kind !== "frame" || round.owner_id !== context.frameID || round.phase !== "output" || round.principal_id !== payload.principalID)
        throw harnessError("FORBIDDEN", "Invalid Run config validation source");
      const selected = runConfigValidationSource(this.store, context.runID, context.frameID, payload.principalID, payload.ref);
      if (!selected || !samePolicyValue(source, selected)) throw harnessError("FORBIDDEN", "Run config source differs from its fixed input binding");
      if (!Number.isSafeInteger(params.maxBytes) || params.maxBytes < 1 || params.maxBytes > 49152) throw harnessError("INVALID_ARGUMENT", "Invalid validation value read size");
      const ref = selected.value, part = new ValueStore(this.store, this.valueQuota).readChunk(ref.id, ref.owner, params.offset, params.maxBytes);
      this.store.assertAuthority(); return { offset: params.offset, data: Buffer.from(part.bytes).toString("base64"), done: part.done };
    }
    if (params.source === "policy") {
      if (!context) throw harnessError("FORBIDDEN", "Policy source context is unavailable");
      const source = storedPolicySource(context), bound = policyPayloadSource(this.store, { context: { actionID: context.actionID ?? null, frameID: context.frameID ?? null } });
      if (!source || !bound || !samePolicyValue(source, bound)) throw harnessError("FORBIDDEN", "Policy source does not match the fixed preparation");
      verifyPolicySource(this.store, source);
      if (!Number.isSafeInteger(params.maxBytes) || params.maxBytes < 1 || params.maxBytes > 49152) throw harnessError("INVALID_ARGUMENT", "Invalid validation value read size");
      const ref = source.value, part = new ValueStore(this.store, this.valueQuota).readChunk(ref.id, ref.owner, params.offset, params.maxBytes);
      this.store.assertAuthority(); return { offset: params.offset, data: Buffer.from(part.bytes).toString("base64"), done: part.done };
    }
    if (params.source === "action") {
      const bound = context?.actionRequestSource;
      if (payload.phase !== "report-output" || !bound || bound.actionID !== context?.actionID || typeof bound.actionID !== "string"
          || payload.config !== null || context?.prepared?.input !== null || context.prepared.config !== null) throw harnessError("FORBIDDEN", "Invalid Action validation source");
      const source = actionRequestValueSource(this.store, bound.actionID);
      if (!source || !samePolicyValue(source, bound.source)) throw harnessError("FORBIDDEN", "Action validation source differs from its fixed request");
      if (!Number.isSafeInteger(params.maxBytes) || params.maxBytes < 1 || params.maxBytes > 49152) throw harnessError("INVALID_ARGUMENT", "Invalid validation value read size");
      const ref = source.value, part = new ValueStore(this.store, this.valueQuota).readChunk(ref.id, ref.owner, params.offset, params.maxBytes);
      this.store.assertAuthority(); return { offset: params.offset, data: Buffer.from(part.bytes).toString("base64"), done: part.done };
    }
    if (payload.kind === "capacity" && payload.phase === "report-output" && context?.operatorSource && params.source !== "report") {
      const source = context.operatorSource as CommandValueSource;
      if (payload.operation !== "admin.resolveAction" || payload.value !== null || source.principalID !== payload.principalID || source.actionID !== context.actionID || context.command !== null
          || !this.store.query("SELECT request_key FROM commands WHERE principal_id=? AND request_key=? AND operation='resolveAction' AND status='pending'", [source.principalID, source.requestKey])[0]) throw harnessError("FORBIDDEN", "Invalid operator validation source");
      verifyCommandValue(this.store, source);
      if (!Number.isSafeInteger(params.maxBytes) || params.maxBytes < 1 || params.maxBytes > 49152) throw harnessError("INVALID_ARGUMENT", "Invalid validation value read size");
      const ref = source.value, part = new ValueStore(this.store, this.valueQuota).readChunk(ref.id, ref.owner, params.offset, params.maxBytes);
      this.store.assertAuthority(); return { offset: params.offset, data: Buffer.from(part.bytes).toString("base64"), done: part.done };
    }
    if (payload.kind === "agent" && payload.phase === "input" && context?.submitSource) {
      if (payload.operation !== "client.submit" || payload.value !== null || payload.config !== null || context.submitSource.principalID !== payload.principalID
          || typeof context.submitSource.requestKey !== "string") throw harnessError("FORBIDDEN", "Invalid submit validation source");
      const source = verifySubmitInput(this.store, payload.principalID, context.submitSource.requestKey, context.request);
      if (!source || canonicalJson(source) !== canonicalJson(context.submitSource)) throw harnessError("FORBIDDEN", "Submit source differs from its command");
      if (!Number.isSafeInteger(params.maxBytes) || params.maxBytes < 1 || params.maxBytes > 49152) throw harnessError("INVALID_ARGUMENT", "Invalid validation value read size");
      const ref = source.value, part = new ValueStore(this.store, this.valueQuota).readChunk(ref.id, ref.owner, params.offset, params.maxBytes);
      this.store.assertAuthority();
      return { offset: params.offset, data: Buffer.from(part.bytes).toString("base64"), done: part.done };
    }
    if (payload.kind === "agent" && payload.phase === "output" && payload.value === null && context?.driverSource) {
      const source = driverValueSource(context.driverSource);
      if (!source || source.frameID !== context.frameID || !this.store.query("SELECT id FROM frames WHERE id=? AND run_id=?", [source.frameID, context.runID])[0])
        throw harnessError("FORBIDDEN", "Driver validation source differs from its frame");
      const ref = verifyDriverValueSource(this.store, source);
      if (!Number.isSafeInteger(params.maxBytes) || params.maxBytes < 1 || params.maxBytes > 49152)
        throw harnessError("INVALID_ARGUMENT", "Invalid validation value read size");
      const part = new ValueStore(this.store, this.valueQuota).readChunk(ref.id, ref.owner, params.offset, params.maxBytes);
      this.store.assertAuthority();
      return { offset: params.offset, data: Buffer.from(part.bytes).toString("base64"), done: part.done };
    }
    if (payload.kind !== "capacity" || payload.phase !== "report-output" || payload.value !== null || !context || typeof context !== "object" || Array.isArray(context)
        || context.reportEncoding !== "report-ref-v1" || typeof context.actionID !== "string")
      throw harnessError("FORBIDDEN", "Validation has no authorized report reference");
    let reportID: unknown, attemptID: unknown, phase: unknown, expectedDigest: unknown;
    if (context.reportPhase === "operator") {
      const command = context.operatorSource ? commandRequestMetadata(this.store.query("SELECT request_json FROM commands WHERE principal_id=? AND request_key=? AND operation='resolveAction'", [payload.principalID, context.operatorSource.requestKey])[0]?.request_json) : context.command, resolution = command?.resolution;
      if (payload.operation !== "admin.resolveAction" || !command || command.actionID !== context.actionID || resolution?.kind !== "legacy_report")
        throw harnessError("FORBIDDEN", "Operator report reference is inconsistent");
      ({ reportID, attemptID, phase, reportDigest: expectedDigest } = resolution);
      if (typeof expectedDigest !== "string" || !/^[0-9a-f]{64}$/.test(expectedDigest))
        throw harnessError("FORBIDDEN", "Operator report digest is invalid");
    } else {
      reportID = context.reportID; attemptID = context.attemptID; phase = context.reportPhase;
    }
    if (typeof reportID !== "string" || typeof attemptID !== "string" || (phase !== "execute" && phase !== "reconcile"))
      throw harnessError("FORBIDDEN", "Report source identity is invalid");
    const row = this.store.query("SELECT * FROM action_reports WHERE id=? AND action_id=? AND attempt_id=? AND phase=?", [reportID, context.actionID, attemptID, phase])[0];
    if (!row) throw harnessError("FORBIDDEN", "Report source is unavailable");
    const ref = actionReportValueReference(this.store, row);
    if (!ref || canonicalJson(context.originalReportRef) !== canonicalJson(ref) || (expectedDigest !== undefined && expectedDigest !== ref.digest))
      throw harnessError("FORBIDDEN", "Report reference does not match its source");
    if (!Number.isSafeInteger(params.maxBytes) || params.maxBytes < 1 || params.maxBytes > 49152)
      throw harnessError("INVALID_ARGUMENT", "Invalid validation value read size");
    const part = new ValueStore(this.store, Number.MAX_SAFE_INTEGER).readChunk(ref.id, ref.owner, params.offset, params.maxBytes);
    this.store.assertAuthority();
    return { offset: params.offset, data: Buffer.from(part.bytes).toString("base64"), done: part.done };
  }

  /** Validate a response fragment again without trusting its frozen data. */
  authorizeValidationValueFragment(params: NodeRPCMap["readValidationValue"]["params"], result: NodeRPCMap["readValidationValue"]["result"], principalID: string, assertWorker: () => void): void {
    if (!result || result.offset !== params.offset || typeof result.data !== "string" || typeof result.done !== "boolean")
      throw harnessError("FORBIDDEN", "Validation value response is invalid");
    const data = Buffer.from(result.data, "base64");
    if (!data.length || data.toString("base64") !== result.data || data.length > params.maxBytes)
      throw harnessError("FORBIDDEN", "Validation value response is invalid");
    const current = this.readValidationValue({ ...params, maxBytes: data.length }, principalID, assertWorker);
    if (current.data !== result.data || current.done !== result.done)
      throw harnessError("EXECUTION_FENCED", "Validation value response changed");
  }
  private releaseLease(params: NodeRPCMap["releaseLease"]["params"], principalID: string): NodeRPCMap["releaseLease"]["result"] {
    if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable");
    if (params.grant.nodeID !== params.nodeID || params.grant.incarnation !== params.incarnation || params.grant.workerPrincipalID !== principalID) throw harnessError("FORBIDDEN", "Lease is not associated with this worker");
    this.leases.release({ ...params.grant, releaseRequestID: params.releaseRequestID });
    return { released: true };
  }

  /** Trusted preparation seam for future Engine use cases; not a public client RPC. */
  createRound(input: PreparationInput): { roundID: string; deadlineAt: number } {
    const request = json(input) as unknown as PreparationInput;
    if (!id(request.roundID) || !id(request.ownerID) || !id(request.principalID) || !id(request.phase)
        || !["command", "frame", "report"].includes(request.ownerKind) || !Number.isSafeInteger(request.timeoutMs) || request.timeoutMs <= 0) {
      throw harnessError("INVALID_ARGUMENT", "Invalid preparation round");
    }
    const jobs = this.jobs(request.jobs, request.principalID);
    const operationID = identity("round.create", [request.roundID]);
    const prior = this.replay(operationID, "validation.round_created", request.roundID, request);
    if (prior) return prior as unknown as { roundID: string; deadlineAt: number };
    const now = this.now();
    const deadlineAt = add(now, request.timeoutMs);
    return this.commit(operationID, "validation.round_created", request.roundID, request, [
      write("INSERT INTO prepare_rounds (id,owner_kind,owner_id,principal_id,phase,number,prepare_timeout_ms,created_at,deadline_at,state) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [request.roundID, request.ownerKind, request.ownerID, request.principalID, request.phase,
          Number(this.store.query("SELECT COALESCE(MAX(number),0) n FROM prepare_rounds WHERE owner_kind=? AND owner_id=?", [request.ownerKind, request.ownerID])[0]!.n) + 1,
          request.timeoutMs, now, deadlineAt, "pending"]),
      ...jobs.map(job => this.insertJob(request.roundID, job)),
    ], { roundID: request.roundID, deadlineAt }, deadlineAt) as unknown as { roundID: string; deadlineAt: number };
  }

  /** Re-preparation stays in the original round and never updates its deadline. */
  replaceJobs(input: { roundID: string; requestID: string; jobs: ValidationJobInput[] }): void {
    const request = json(input) as unknown as typeof input;
    if (!id(request.roundID) || !id(request.requestID)) throw harnessError("INVALID_ARGUMENT", "Invalid replacement identity");
    const operationID = identity("round.replace", [request.roundID, request.requestID]);
    if (this.replay(operationID, "validation.jobs_replaced", request.roundID, request)) return;
    const round = this.round(request.roundID, this.now());
    const jobs = this.jobs(request.jobs, round.principal_id as string);
    const old = this.store.query("SELECT id FROM validation_jobs WHERE round_id=? AND state<>?", [request.roundID, "superseded"]);
    this.commit(operationID, "validation.jobs_replaced", request.roundID, request, [
      this.roundGuard(request.roundID, this.now()),
      write("UPDATE validation_jobs SET state=? WHERE round_id=? AND state<>?", ["superseded", request.roundID, "superseded"], old.length),
      ...jobs.map(job => this.insertJob(request.roundID, job)),
    ], { replaced: true }, round.deadline_at as number);
  }

  /** Caller owns command/frame expiry policy. Closing an unassigned job is valid too. */
  endRound(roundID: string, state: "expired" | "superseded"): void {
    if (!id(roundID) || !["expired", "superseded"].includes(state)) throw harnessError("INVALID_ARGUMENT", "Invalid round closure");
    const request = { roundID, state };
    const operationID = identity("round.end", [roundID, state]);
    if (this.replay(operationID, "validation.round_closed", roundID, request)) return;
    const rows = this.store.query("SELECT id FROM validation_jobs WHERE round_id=? AND state<>?", [roundID, "superseded"]);
    this.commit(operationID, "validation.round_closed", roundID, request, [
      write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=?", [state, roundID, "pending"]),
      write("UPDATE validation_jobs SET state=? WHERE round_id=? AND state<>?", ["superseded", roundID, "superseded"], rows.length),
    ], { closed: true });
  }

  private async register(params: NodeRPCMap["register"]["params"], principalID: string, assertWorker: () => void = () => {}): Promise<NodeSession> {
    const operationID = identity("node.register", [principalID, params.nodeID, params.incarnation]);
    const prior = this.store.query("SELECT request_json,result_json,type,subject_id FROM history WHERE operation_id=?", [operationID])[0];
    if (prior) {
      const original = parseJson(prior.request_json as string) as Record<string, Json>, { principalID: owner, ...configuration } = original;
      if (prior.type !== "node.registered" || prior.subject_id !== params.nodeID || owner !== principalID || !samePolicyValue(readNodeConfiguration(this.store, configuration), params)) throw harnessError("REQUEST_CONFLICT", "Registration identity belongs to different content");
      this.session(params, principalID, this.now()); assertWorker(); return parseJson(prior.result_json as string) as unknown as NodeSession;
    }
    // Admission limits apply to new registrations, not a previously committed receipt.
    for (const policy of params.policies) assertJsonValueSize(policy.config, this.maxValueBytes);
    const existing = this.store.query("SELECT node_id,principal_id FROM node_sessions WHERE incarnation=?", [params.incarnation])[0];
    if (existing) {
      if (existing.node_id !== params.nodeID || existing.principal_id !== principalID) throw harnessError("FORBIDDEN", "Incarnation belongs to another node or worker");
      throw harnessError("NODE_STATE_INVALID", "This incarnation was closed before registration completed");
    }
    if (!params.definitions.some(definition => definition.kind !== "policy")) throw harnessError("INVALID_ARGUMENT", "A node needs an Agent or Capacity definition");
    const seen = new Set<string>();
    for (const definition of params.definitions) {
      if (definition.kind !== "capacity" && definition.supportsReconcile !== undefined) throw harnessError("INVALID_ARGUMENT", "Only Capacity may declare reconciliation support");
      const key = canonicalJson({ kind: definition.kind, ref: definition.ref });
      if (seen.has(key)) throw harnessError("DUPLICATE_DEFINITION", "Duplicate node definition");
      seen.add(key);
    }
    const selectedPolicies = new Set<string>();
    for (const policy of params.policies) {
      const selected = canonicalJson(policy.ref);
      if (selectedPolicies.has(selected)) throw harnessError("DUPLICATE_DEFINITION", "Duplicate selected policy");
      selectedPolicies.add(selected);
      if (!seen.has(canonicalJson({ kind: "policy", ref: policy.ref }))) throw harnessError("VERSION_UNAVAILABLE", "A selected policy is not installed on this node");
    }
    const source = await stageNodePolicies(this.store, principalID, params, this.valueQuota, assertWorker);
    try {
    this.store.assertAuthority(); assertWorker();
    if (this.store.getCommit(operationID)) return this.register(params, principalID, assertWorker);
    const now = this.now(), configuration = encodeNodeConfiguration(params, source), request = { principalID, ...(configuration as Record<string, Json>) };
    const previous = this.store.query("SELECT incarnation,expires_at FROM node_sessions WHERE node_id=? AND state=?", [params.nodeID, "online"]);
    if (previous.some(session => (session.expires_at as number) > now)) throw harnessError("NODE_STATE_INVALID", "An active node session already owns this nodeID");
    const expiresAt = add(now, this.options.sessionTTLms);
    const result = this.nodeSession(params, now, expiresAt);
    const writes: SQLWrite[] = [write("UPDATE node_sessions SET state=? WHERE node_id=? AND state=? AND expires_at<=?", ["offline", params.nodeID, "online", now], previous.length),
      write("INSERT INTO node_sessions (incarnation,node_id,principal_id,state,expires_at,registered_at,configuration_json) VALUES (?,?,?,?,?,?,?)",
        [params.incarnation, params.nodeID, principalID, "online", expiresAt, now, { json: configuration }]),
      write("INSERT INTO node_claim_gates(incarnation,enabled) VALUES(?,?)", [params.incarnation, 1])];
    const absent: NonNullable<CommitBatch["absent"]> = [];
    for (const definition of params.definitions) {
      if (!this.store.query("SELECT kind FROM definition_catalog WHERE kind=? AND id=? AND version=?", [definition.kind, definition.ref.id, definition.ref.version]).length) {
        writes.push(write("INSERT INTO definition_catalog (kind,id,version,registered_at) VALUES (?,?,?,?)", [definition.kind, definition.ref.id, definition.ref.version, now]));
      }
      writes.push(write("INSERT INTO node_definitions (incarnation,kind,id,version) VALUES (?,?,?,?)", [params.incarnation, definition.kind, definition.ref.id, definition.ref.version]));
      if (definition.kind === "capacity" && definition.supportsReconcile !== undefined) {
        const existing = this.store.query("SELECT supported FROM capacity_reconciliation_support WHERE id=? AND version=?", [definition.ref.id, definition.ref.version])[0];
        const supported = definition.supportsReconcile ? 1 : 0;
        if (existing && existing.supported !== supported) throw harnessError("DUPLICATE_DEFINITION", "Fixed Capacity version has conflicting reconciliation support");
        if (existing) writes.push(write("UPDATE capacity_reconciliation_support SET revision=revision+0 WHERE id=? AND version=? AND supported=?", [definition.ref.id, definition.ref.version, supported]));
        else {
          absent.push({ table: "capacity_reconciliation_support", id: definition.ref.id, version: definition.ref.version });
          writes.push(write("INSERT INTO capacity_reconciliation_support(id,version,supported) VALUES(?,?,?)", [definition.ref.id, definition.ref.version, supported]));
        }
      }
    }
    try {
      const committed = await this.store.commit({ operationID, kind: "node.registered", subjectID: params.nodeID, request: json(request), writes: [...writes, ...policyPublicationWrites(source, this.store.coordinatorEpoch)],
        evidence: null, result: json(result), validBefore: expiresAt, ...(source ? { valuePublications: [source.value] } : {}), ...(absent.length ? { absent } : {}) });
      return committed.result as unknown as NodeSession;
    }
    catch (error) {
      if (error && typeof error === "object" && "info" in error && (error.info as { code?: string; disposition?: string }).code === "REVISION_CONFLICT" && (error.info as { disposition?: string }).disposition !== "uncertain") {
        this.store.assertAuthority(); return this.register(params, principalID, assertWorker);
      }
      throw error;
    }
    } finally { abandonNodePolicies(this.store, source); }
  }

  private renew(params: NodeRPCMap["renew"]["params"], principalID: string): NodeSession {
    const request = { principalID, ...params };
    const operationID = identity("node.renew", [principalID, params.incarnation, params.renewRequestID]);
    const prior = this.replay(operationID, "node.renewed", params.nodeID, request);
    const now = this.now();
    const session = this.session(params, principalID, now);
    if (prior) return prior as unknown as NodeSession;
    const expiresAt = add(now, this.options.sessionTTLms);
    return this.commit(operationID, "node.renewed", params.nodeID, request, [
      write("UPDATE node_sessions SET expires_at=? WHERE incarnation=? AND node_id=? AND principal_id=? AND state=? AND expires_at=? AND expires_at>?",
        [expiresAt, params.incarnation, params.nodeID, principalID, "online", session.expires_at as number, now]),
    ], this.nodeSession(params, now, expiresAt), session.expires_at as number) as unknown as NodeSession;
  }

  private quiesce(params: NodeRPCMap["quiesce"]["params"], principalID: string): { quiesced: true } {
    const request = { principalID, ...params }, operationID = identity("node.quiesce", [principalID, params.incarnation]);
    const prior = this.replay(operationID, "node.quiesced", params.nodeID, request);
    if (prior) return prior as unknown as { quiesced: true };
    const now = this.now(), session = this.session(params, principalID, now);
    const writes = [
      this.sessionGuard(params, principalID, session.expires_at as number, now),
      write("UPDATE node_claim_gates SET enabled=? WHERE incarnation=? AND enabled=?", [0, params.incarnation, 1]),
    ];
    // A stopped local loop cannot consume its outstanding claim. If that claim
    // already committed, use its original receipt to fence only that assignment.
    // If it has not committed, the gate CAS above wins against the claim CAS.
    const claimID = params.pendingClaimRequestID === undefined ? null : identity("node.claim", [principalID, params.incarnation, params.pendingClaimRequestID]);
    const claim = claimID === null ? null : this.store.getCommit(claimID);
    if (claim) {
      if (claim.kind !== "validation.claimed" || claim.subjectID !== params.nodeID) throw harnessError("INTERNAL_ERROR", "Pending claim receipt is invalid");
      const stored = (claim.result as unknown as { assignment: NodeAssignment | { encoding: string; assignment: NodeAssignment } | null }).assignment;
      // Quiesce only needs the original lease and assignment identifiers. A
      // frame-reference receipt retains them without materializing its input.
      const assignment = stored && "encoding" in stored && ["frame-assignment-v1", "child-frame-assignment-v1", "action-assignment-v1"].includes(stored.encoding) ? stored.assignment : stored as NodeAssignment | null;
      if (assignment) {
        if (assignment.nodeID !== params.nodeID || assignment.incarnation !== params.incarnation) throw harnessError("INTERNAL_ERROR", "Pending claim belongs to another node");
        if ("lease" in assignment) {
          const grant = assignment.lease;
          const current = this.store.query("SELECT * FROM business_leases WHERE kind=? AND subject_id=?", [grant.kind, grant.subjectID])[0];
          if (current && current.node_id === grant.nodeID && current.worker_principal_id === principalID && current.incarnation === grant.incarnation
              && current.epoch === grant.epoch && current.attempt_id === grant.attemptID && current.attempt_number === grant.attemptNumber && current.released === 0) {
            writes.push(write("UPDATE business_leases SET released=1 WHERE kind=? AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND released=0 AND attempt_id IS ? AND attempt_number IS ?", [grant.kind, grant.subjectID, grant.nodeID, principalID, grant.incarnation, grant.epoch, grant.attemptID, grant.attemptNumber]));
          }
          if ("driver" in assignment) {
            const job = this.store.query("SELECT state,assignment_id,assignment_epoch,node_incarnation,assignment_deadline_at FROM driver_jobs WHERE id=?", [assignment.jobID])[0];
            if (job?.state === "assigned" && job.assignment_id === assignment.assignmentID && job.assignment_epoch === assignment.assignmentEpoch && job.node_incarnation === params.incarnation) {
              writes.push(write("UPDATE driver_jobs SET assignment_deadline_at=? WHERE id=? AND state='assigned' AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND assignment_deadline_at=?", [Math.min(now, job.assignment_deadline_at as number), assignment.jobID, assignment.assignmentID, assignment.assignmentEpoch, params.incarnation, job.assignment_deadline_at as number]));
            }
          }
        } else {
          const job = this.store.query("SELECT state,assignment_id,assignment_epoch,node_incarnation,assignment_deadline_at FROM validation_jobs WHERE id=?", [assignment.jobID])[0];
          if (job?.state === "assigned" && job.assignment_id === assignment.assignmentID && job.assignment_epoch === assignment.assignmentEpoch && job.node_incarnation === params.incarnation) {
            writes.push(write("UPDATE validation_jobs SET assignment_deadline_at=? WHERE id=? AND state='assigned' AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND assignment_deadline_at=?", [Math.min(now, job.assignment_deadline_at as number), assignment.jobID, assignment.assignmentID, assignment.assignmentEpoch, params.incarnation, job.assignment_deadline_at as number]));
          }
        }
      }
    }
    try {
      return this.commit(operationID, "node.quiesced", params.nodeID, request, writes, { quiesced: true }, session.expires_at as number,
        undefined, claimID && !claim ? [{ table: "claim_receipt", id: claimID }] : undefined) as unknown as { quiesced: true };
    } catch (error) {
      // A claim may commit after our receipt read but before the write lock.
      // The absence guard rolls back the gate; reread its receipt and retry.
      if (claimID && !claim && error instanceof HarnessError && error.info.code === "REVISION_CONFLICT" && this.store.getCommit(claimID)) return this.quiesce(params, principalID);
      throw error;
    }
  }

  private async claim(params: NodeRPCMap["claim"]["params"], principalID: string, assertWorker: () => void): Promise<{ assignment: NodeAssignment | null }> {
    const request = { principalID, ...params };
    const operationID = identity("node.claim", [principalID, params.incarnation, params.claimRequestID]);
    const prior = this.replay(operationID, "validation.claimed", params.nodeID, request);
    let now = this.now();
    let session = this.session(params, principalID, now);
    const assertClaimGate = () => { if (this.store.query("SELECT enabled FROM node_claim_gates WHERE incarnation=?", [params.incarnation])[0]?.enabled !== 1) throw harnessError("NODE_STATE_INVALID", "Node has stopped claiming work"); };
    const verifyReplay = async (stored: unknown) => {
      const result = await this.readClaimResult(stored);
      if (result.assignment) await this.checkReplayedAssignment(result.assignment, this.now());
      assertWorker(); this.store.assertAuthority();
      now = this.now(); session = this.session(params, principalID, now); assertClaimGate();
      if (result.assignment) this.checkReplayedAssignmentCurrent(result.assignment, now);
      return result;
    };
    assertClaimGate();
    if (prior) {
      const result = prior as unknown as { assignment: NodeAssignment | null };
      return verifyReplay(result);
    }
    let cursor = "";
    let candidate: ReturnType<SQLiteStore["query"]>[number] | undefined;
    let candidatePayload: ValidationPayload | undefined;
    while (params.validationAvailable !== false) {
      assertWorker();
      // Authorization can filter a whole page. Advance past it instead of
      // repeatedly selecting the same revoked subjects on every claim.
      const rows = this.store.query(`SELECT j.id,j.round_id,j.payload_json,j.state,j.assignment_epoch,j.assignment_deadline_at,j.node_incarnation,r.deadline_at,r.principal_id FROM validation_jobs j
        JOIN prepare_rounds r ON r.id=j.round_id
        JOIN node_definitions d ON d.incarnation=? AND d.kind=CASE WHEN json_extract(j.payload_json,'$.phase')='rewrite' THEN 'policy' ELSE json_extract(j.payload_json,'$.kind') END
          AND d.id=CASE WHEN json_extract(j.payload_json,'$.phase')='rewrite' THEN json_extract(j.payload_json,'$.context.policies[0].ref.id') ELSE json_extract(j.payload_json,'$.ref.id') END
          AND d.version=CASE WHEN json_extract(j.payload_json,'$.phase')='rewrite' THEN json_extract(j.payload_json,'$.context.policies[0].ref.version') ELSE json_extract(j.payload_json,'$.ref.version') END
        WHERE r.state='pending' AND r.deadline_at>? AND j.id>? AND (j.state='pending' OR (j.state='assigned'
          AND (j.assignment_deadline_at<=? OR NOT EXISTS (SELECT 1 FROM node_sessions s
            WHERE s.incarnation=j.node_incarnation AND s.state='online' AND s.expires_at>?)))) ORDER BY j.id LIMIT 1`, [params.incarnation, now, cursor, now, now]);
      for (const row of rows) {
        const payload = validateValidationPayload(parseJson(row.payload_json as string));
        if (payload.principalID !== row.principal_id) throw harnessError("INTERNAL_ERROR", "Validation subject does not match its preparation round");
        if (!this.supportsPreparation(params.incarnation, payload)) continue;
        try { this.authorizePayload(payload); } catch (error) {
          if (error && typeof error === "object" && "info" in error && (error.info as { code?: string }).code === "FORBIDDEN") continue;
          throw error;
        }
        candidate = row;
        candidatePayload = payload;
        break;
      }
      if (candidate || rows.length === 0) break;
      cursor = rows.at(-1)!.id as string;
      await new Promise<void>(resolve => setImmediate(resolve));
      assertWorker();
      this.store.assertAuthority();
      now = this.now();
      session = this.session(params, principalID, now);
      assertClaimGate();
      const replayed = this.replay(operationID, "validation.claimed", params.nodeID, request);
      if (replayed) {
        const result = replayed as unknown as { assignment: NodeAssignment | null };
        return verifyReplay(result);
      }
    }
    let assignment: NodeAssignment | null = null;
    let persistedAssignment: Json | undefined;
    const writes = [this.sessionGuard(params, principalID, session.expires_at as number, now),
      write("UPDATE node_claim_gates SET enabled=? WHERE incarnation=? AND enabled=?", [1, params.incarnation, 1])];
    const events: HistoryEvent[] = [];
    if (candidate) {
      const row = candidate;
      const payload = candidatePayload!;
      const epoch = add(row.assignment_epoch as number, 1);
      assignment = { jobID: row.id as string, roundID: row.round_id as string, payload,
        assignmentID: randomUUID(), assignmentEpoch: epoch, nodeID: params.nodeID, incarnation: params.incarnation,
        deadlineAt: Math.min(add(now, this.options.assignmentTTLms), row.deadline_at as number), roundDeadlineAt: row.deadline_at as number };
      writes.push(this.roundGuard(assignment.roundID, now));
      if (row.state === "assigned" && (row.assignment_deadline_at as number) > now) {
        const previous = this.store.query("SELECT state,expires_at FROM node_sessions WHERE incarnation=?", [row.node_incarnation!])[0];
        if (!previous || previous.state === "online" && (previous.expires_at as number) > now) throw harnessError("REVISION_CONFLICT", "Former node session changed during allocation");
        writes.push(write("UPDATE node_sessions SET expires_at=? WHERE incarnation=? AND state=? AND expires_at=?",
          [previous.expires_at as number, row.node_incarnation as string, previous.state as string, previous.expires_at as number]));
      }
      // A revoked/expired former owner cannot win a report against this CAS.
      writes.push(write("UPDATE validation_jobs SET state=?,assignment_id=?,assignment_epoch=assignment_epoch+1,node_incarnation=?,assignment_deadline_at=? WHERE id=? AND state=? AND assignment_epoch=?",
        ["assigned", assignment.assignmentID, params.incarnation, assignment.deadlineAt, assignment.jobID, row.state as string, row.assignment_epoch as number]));
    }
    let leaseDeadline: number | undefined;
    if (!assignment && params.runAvailable !== false && this.leases) {
      const definitions = this.getDefinitions(params.nodeID, params.incarnation).filter(definition => definition.kind === "agent");
      for (const definition of definitions) {
        let startCursor = "";
        for (;;) {
        assertWorker();
        const rows = this.store.query(`SELECT j.id,j.principal_id,j.agent_id,j.agent_version,j.assignment_epoch,j.state FROM start_jobs j JOIN runs r ON r.id=j.id
          JOIN work_items w ON w.kind='run' AND w.subject_id=j.id AND w.stage='start'
          LEFT JOIN business_leases l ON l.kind='run' AND l.subject_id=j.id
          WHERE j.coordinator_epoch=? AND j.agent_id=? AND j.agent_version=? AND j.state IN ('pending','assigned','accepted')
          AND r.core_json IS NULL AND r.business_cancelled=0 AND r.business_terminal=0
          AND (l.subject_id IS NULL OR l.released=1 OR l.expires_at<=?) AND j.id>? ORDER BY j.id LIMIT 1`, [this.store.coordinatorEpoch, definition.ref.id, definition.ref.version, now, startCursor]);
        for (const row of rows) {
          if (this.store.effectiveCancellation({ kind: "run", id: row.id as string })) continue;
          const target = { id: row.agent_id as string, version: row.agent_version as string };
          try { this.authorizePayload({ kind: "agent", ref: target, phase: "input", value: null, config: null, principalID: row.principal_id as string, operation: "client.submit", context: null }); }
          catch (error) {
            if (error && typeof error === "object" && "info" in error && (error.info as { code?: string }).code === "FORBIDDEN") continue;
            throw error;
          }
          const sampledAt = this.now();
          const prepared = this.leases.prepareClaim({ kind: "run", subjectID: row.id as string, nodeID: params.nodeID, incarnation: params.incarnation, workerPrincipalID: principalID, claimRequestID: params.claimRequestID });
          if (!prepared.grant) continue;
          assignment = { start: true, jobID: row.id as string, runID: row.id as string, principalID: row.principal_id as string, agent: target,
            assignmentID: randomUUID(), assignmentEpoch: add(row.assignment_epoch as number, 1), nodeID: params.nodeID, incarnation: params.incarnation,
            deadlineAt: prepared.grant.expiresAt, lease: prepared.grant, leaseTTLms: prepared.grant.expiresAt - sampledAt };
          writes.push(...prepared.writes,
            write("UPDATE work_items SET stage=? WHERE kind=? AND subject_id=? AND stage=?", ["start", "run", assignment.runID, "start"]),
            write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND core_json IS NULL", [assignment.runID]),
            write("UPDATE start_jobs SET state=?,assignment_id=?,assignment_epoch=assignment_epoch+1,node_incarnation=?,assignment_deadline_at=?,assignment_json=?,grant_json=NULL,lease_epoch=? WHERE id=? AND state=? AND assignment_epoch=? AND coordinator_epoch=?", ["assigned", assignment.assignmentID, params.incarnation, assignment.deadlineAt, { json: json(assignment) }, prepared.grant.epoch, assignment.jobID, row.state as string, row.assignment_epoch as number, this.store.coordinatorEpoch]));
          leaseDeadline = prepared.validBefore;
          break;
        }
        if (assignment || rows.length === 0) break;
        startCursor = rows.at(-1)!.id as string;
        await new Promise<void>(resolve => setImmediate(resolve));
        assertWorker(); this.store.assertAuthority();
        now = this.now(); session = this.session(params, principalID, now); assertClaimGate();
        writes[0] = this.sessionGuard(params, principalID, session.expires_at as number, now);
        const replayed = this.replay(operationID, "validation.claimed", params.nodeID, request);
        if (replayed) {
          const result = replayed as unknown as { assignment: NodeAssignment | null };
          return verifyReplay(result);
        }
        }
        if (assignment) break;
      }
    }
    if (!assignment && params.runAvailable !== false && this.leases) {
      const definitions = this.getDefinitions(params.nodeID, params.incarnation).filter(definition => definition.kind === "agent");
      for (const definition of definitions) {
        let wakeCursor = "";
        for (;;) {
          assertWorker();
          const rows = this.store.query(`SELECT j.id,j.run_id,j.principal_id,j.agent_id,j.agent_version,j.assignment_epoch,j.state,t.winner,t.wake_json,r.core_json FROM wake_jobs j
            JOIN waits t ON t.id=j.id AND t.run_id=j.run_id AND t.winner IS NOT NULL AND t.wake_json IS NOT NULL AND t.resumed_frame_id IS NULL
            JOIN runs r ON r.id=j.run_id AND r.business_cancelled=0 AND r.business_terminal=0
            JOIN work_items w ON w.kind='run' AND w.subject_id=j.run_id AND w.stage='wake'
            LEFT JOIN business_leases l ON l.kind='run' AND l.subject_id=j.run_id
            WHERE j.agent_id=? AND j.agent_version=? AND j.state IN ('pending','assigned','accepted')
              AND (l.subject_id IS NULL OR l.released=1 OR l.expires_at<=?) AND j.id>? ORDER BY j.id LIMIT 1`, [definition.ref.id, definition.ref.version, now, wakeCursor]);
          for (const row of rows) {
            if (this.store.effectiveCancellation({ kind: "run", id: row.run_id as string })) continue;
            if (!this.canWake(row.run_id as string)) continue;
            const target = { id: row.agent_id as string, version: row.agent_version as string };
            try { this.authorizePayload({ kind: "agent", ref: target, phase: "input", value: null, config: null, principalID: row.principal_id as string, operation: "client.submit", context: null }); }
            catch (error) {
              if (error && typeof error === "object" && "info" in error && (error.info as { code?: string }).code === "FORBIDDEN") continue;
              throw error;
            }
            const sampledAt = this.now();
            const prepared = this.leases.prepareClaim({ kind: "run", subjectID: row.run_id as string, nodeID: params.nodeID, incarnation: params.incarnation, workerPrincipalID: principalID, claimRequestID: params.claimRequestID });
            if (!prepared.grant) continue;
            assignment = { wake: true, jobID: row.id as string, waitID: row.id as string, runID: row.run_id as string, principalID: row.principal_id as string, agent: target,
              assignmentID: randomUUID(), assignmentEpoch: add(row.assignment_epoch as number, 1), nodeID: params.nodeID, incarnation: params.incarnation,
              deadlineAt: prepared.grant.expiresAt, lease: prepared.grant, leaseTTLms: prepared.grant.expiresAt - sampledAt };
            writes.push(...prepared.writes,
              write("UPDATE waits SET winner=? WHERE id=? AND run_id=? AND winner=? AND wake_json=? AND resumed_frame_id IS NULL", [row.winner as string, assignment.waitID, assignment.runID, row.winner as string, row.wake_json as string]),
              write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND core_json=?", [assignment.runID, row.core_json as string]),
              write("UPDATE work_items SET stage=? WHERE kind=? AND subject_id=? AND stage=?", ["wake", "run", assignment.runID, "wake"]),
              write("UPDATE wake_jobs SET state=?,assignment_id=?,assignment_epoch=assignment_epoch+1,node_incarnation=?,assignment_deadline_at=?,assignment_json=?,grant_json=NULL,lease_epoch=? WHERE id=? AND run_id=? AND state=? AND assignment_epoch=?", ["assigned", assignment.assignmentID, params.incarnation, assignment.deadlineAt, { json: json(assignment) }, prepared.grant.epoch, assignment.waitID, assignment.runID, row.state as string, row.assignment_epoch as number]));
            leaseDeadline = prepared.validBefore;
            break;
          }
          if (assignment || rows.length === 0) break;
          wakeCursor = rows.at(-1)!.id as string;
          await new Promise<void>(resolve => setImmediate(resolve));
          assertWorker(); this.store.assertAuthority();
          now = this.now(); session = this.session(params, principalID, now); assertClaimGate();
          writes[0] = this.sessionGuard(params, principalID, session.expires_at as number, now);
          const replayed = this.replay(operationID, "validation.claimed", params.nodeID, request);
          if (replayed) {
            const result = replayed as unknown as { assignment: NodeAssignment | null };
            return verifyReplay(result);
          }
        }
        if (assignment) break;
      }
    }
    if (!assignment && params.runAvailable !== false && this.leases) {
      const definitions = this.getDefinitions(params.nodeID, params.incarnation).filter(definition => definition.kind === "agent");
      for (const definition of definitions) {
        let finalizeCursor = "";
        for (;;) {
        assertWorker();
        const rows = this.store.query(`SELECT j.id,j.run_id,j.principal_id,j.input_json,j.input_encoding,j.assignment_epoch,j.state FROM finalize_jobs j LEFT JOIN business_leases l ON l.kind='run' AND l.subject_id=j.run_id
          WHERE j.coordinator_epoch=? AND j.agent_id=? AND j.agent_version=? AND j.state IN ('pending','assigned','accepted')
          AND (l.subject_id IS NULL OR l.released=1 OR l.expires_at<=?) AND j.id>? ORDER BY j.id LIMIT 1`, [this.store.coordinatorEpoch, definition.ref.id, definition.ref.version, now, finalizeCursor]);
        for (const row of rows) {
          if (this.store.effectiveCancellation({ kind: "run", id: row.run_id as string })) continue;
          if (!this.canDrive(row.run_id as string)) continue;
          const frame = this.store.query("SELECT id,run_id,input_json,input_encoding,input_operation_id FROM frames WHERE id=? AND run_id=? AND state='open'", [row.id as string, row.run_id as string])[0];
          const work = this.store.query("SELECT id,stage FROM work_items WHERE kind='run' AND subject_id=? AND stage IN ('driver','finalize')", [row.run_id as string])[0];
          const inputJSON = frame && frameJobInput(row, frame);
          if (!inputJSON || !work) continue;
          const input = await readFrameInput(this.store, frame!);
          assertWorker(); this.store.assertAuthority(); now = this.now(); session = this.session(params, principalID, now); assertClaimGate();
          writes[0] = this.sessionGuard(params, principalID, session.expires_at as number, now);
          if (this.store.effectiveCancellation({ kind: "run", id: row.run_id as string }) || !this.canDrive(row.run_id as string)) continue;
          try { this.authorizeDriver(row.principal_id as string, input); }
          catch (error) {
            if (error && typeof error === "object" && "info" in error && (error.info as { code?: string }).code === "FORBIDDEN") continue;
            throw error;
          }
          const sampledAt = this.now();
          const prepared = this.leases.prepareClaim({ kind: "run", subjectID: row.run_id as string, nodeID: params.nodeID, incarnation: params.incarnation, workerPrincipalID: principalID, claimRequestID: params.claimRequestID });
          if (!prepared.grant) continue;
          assignment = { finalize: true, jobID: row.id as string, runID: row.run_id as string, principalID: row.principal_id as string,
            input, assignmentID: randomUUID(), assignmentEpoch: add(row.assignment_epoch as number, 1), nodeID: params.nodeID, incarnation: params.incarnation,
            deadlineAt: prepared.grant.expiresAt, lease: prepared.grant, leaseTTLms: prepared.grant.expiresAt - sampledAt };
          persistedAssignment = this.frameAssignmentValue(assignment, frame!);
          writes.push(...prepared.writes,
            this.frameGuard(assignment, frame!),
            write("UPDATE work_items SET stage=? WHERE id=? AND stage=?", [work.stage as string, work.id as string, work.stage as string]),
            write("UPDATE finalize_jobs SET state=?,assignment_id=?,assignment_epoch=assignment_epoch+1,node_incarnation=?,assignment_deadline_at=?,assignment_json=?,assignment_encoding=?,grant_json=NULL,lease_epoch=? WHERE id=? AND state=? AND assignment_epoch=? AND coordinator_epoch=?", ["assigned", assignment.assignmentID, params.incarnation, assignment.deadlineAt, { json: persistedAssignment }, frame!.input_encoding === "wait-ref-v1" ? "frame-ref-v1" : "inline", prepared.grant.epoch, assignment.jobID, row.state as string, row.assignment_epoch as number, this.store.coordinatorEpoch]));
          leaseDeadline = prepared.validBefore;
          break;
        }
        if (assignment || rows.length === 0) break;
        finalizeCursor = rows.at(-1)!.id as string;
        await new Promise<void>(resolve => setImmediate(resolve));
        assertWorker(); this.store.assertAuthority();
        now = this.now(); session = this.session(params, principalID, now); assertClaimGate();
        writes[0] = this.sessionGuard(params, principalID, session.expires_at as number, now);
        const replayed = this.replay(operationID, "validation.claimed", params.nodeID, request);
        if (replayed) {
          const result = replayed as unknown as { assignment: NodeAssignment | null };
          return verifyReplay(result);
        }
        }
        if (assignment) break;
      }
    }
    if (!assignment && params.runAvailable !== false) {
      let row: ReturnType<SQLiteStore["query"]>[number] | undefined;
      let selectedInput: ReturnType<typeof validateAdvanceInput> | undefined;
      let selectedFrame: ReturnType<SQLiteStore["query"]>[number] | undefined;
      // The route prefix and keyset keep unrelated versions and waiting jobs out
      // of each page. An offline owner is retried after its finite deadline.
      const definitions = this.getDefinitions(params.nodeID, params.incarnation).filter(definition => definition.kind === "agent");
      for (const definition of definitions) {
        for (const state of ["pending", "assigned"] as const) {
          let cursor = "", deadlineCursor = 0;
          for (;;) {
            assertWorker();
            const rows = state === "pending"
              ? this.store.query(`SELECT id,run_id,principal_id,input_json,input_encoding,assignment_epoch,state,assignment_deadline_at FROM driver_jobs WHERE coordinator_epoch=? AND state='pending' AND agent_id=? AND agent_version=? AND id>?
                  ORDER BY id LIMIT 1`, [this.store.coordinatorEpoch, definition.ref.id, definition.ref.version, cursor])
              : this.store.query(`SELECT id,run_id,principal_id,input_json,input_encoding,assignment_epoch,state,assignment_deadline_at FROM driver_jobs WHERE coordinator_epoch=? AND state='assigned' AND agent_id=? AND agent_version=? AND assignment_deadline_at<=?
                  AND (assignment_deadline_at,id)>(?,?) ORDER BY assignment_deadline_at,id LIMIT 1`,
                  [this.store.coordinatorEpoch, definition.ref.id, definition.ref.version, now, deadlineCursor, cursor]);
            for (const possible of rows) {
              if (this.store.effectiveCancellation({ kind: "run", id: possible.run_id as string })) continue;
              if (!this.canDrive(possible.run_id as string)) continue;
              const active = this.store.query(`SELECT f.id,f.run_id,f.input_json,f.input_encoding,f.input_operation_id FROM frames f JOIN work_items w ON w.kind='run' AND w.subject_id=f.run_id AND w.stage='driver'
                WHERE f.id=? AND f.run_id=? AND f.state='open'`, [possible.id as string, possible.run_id as string])[0];
              const inputJSON = active && frameJobInput(possible, active);
              if (!inputJSON) continue;
              const input = await readFrameInput(this.store, active!);
              assertWorker(); this.store.assertAuthority(); now = this.now(); session = this.session(params, principalID, now); assertClaimGate();
              if (this.store.effectiveCancellation({ kind: "run", id: possible.run_id as string }) || !this.canDrive(possible.run_id as string)) continue;
              try { this.authorizeDriver(possible.principal_id as string, input); } catch (error) {
                if (error && typeof error === "object" && "info" in error && (error.info as { code?: string }).code === "FORBIDDEN") continue;
                throw error;
              }
              row = possible;
              selectedInput = input;
              selectedFrame = active;
              break;
            }
            if (row || rows.length === 0) break;
            cursor = rows.at(-1)!.id as string;
            if (state === "assigned") deadlineCursor = rows.at(-1)!.assignment_deadline_at as number;
            await new Promise<void>(resolve => setImmediate(resolve));
            assertWorker();
            this.store.assertAuthority();
            now = this.now();
            session = this.session(params, principalID, now);
            assertClaimGate();
            const replayed = this.replay(operationID, "validation.claimed", params.nodeID, request);
            if (replayed) {
              const result = replayed as unknown as { assignment: NodeAssignment | null };
              return verifyReplay(result);
            }
          }
          if (row) break;
        }
        if (row) break;
      }
      writes[0] = this.sessionGuard(params, principalID, session.expires_at as number, now);
      if (row && this.canDrive(row.run_id as string)) {
        if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable");
        const input = selectedInput!;
        this.authorizeDriver(row.principal_id as string, input);
        const sampledAt = this.now();
        const prepared = this.leases.prepareClaim({ kind: "run", subjectID: row.run_id as string, nodeID: params.nodeID, incarnation: params.incarnation, workerPrincipalID: principalID, claimRequestID: params.claimRequestID });
        if (!prepared.grant) return this.commit(operationID, "validation.claimed", params.nodeID, request, [...writes, ...prepared.writes], { assignment: null }, prepared.validBefore) as unknown as { assignment: NodeAssignment | null };
        assignment = { driver: true, jobID: row.id as string, runID: row.run_id as string, principalID: row.principal_id as string,
          input, assignmentID: randomUUID(), assignmentEpoch: add(row.assignment_epoch as number, 1),
          nodeID: params.nodeID, incarnation: params.incarnation, deadlineAt: prepared.grant.expiresAt, lease: prepared.grant, leaseTTLms: prepared.grant.expiresAt - sampledAt };
        persistedAssignment = this.frameAssignmentValue(assignment, selectedFrame!);
        writes.push(...prepared.writes);
        leaseDeadline = prepared.validBefore;
        writes.push(...this.driverGuards(assignment, selectedFrame!));
        writes.push(write("UPDATE driver_jobs SET state=?,assignment_id=?,assignment_epoch=assignment_epoch+1,node_incarnation=?,assignment_deadline_at=?,assignment_json=?,assignment_encoding=?,grant_json=NULL,report_json=NULL,lease_epoch=? WHERE id=? AND state=? AND assignment_epoch=? AND coordinator_epoch=?",
          ["assigned", assignment.assignmentID, params.incarnation, assignment.deadlineAt, { json: persistedAssignment }, selectedFrame!.input_encoding === "wait-ref-v1" ? "frame-ref-v1" : "inline", prepared.grant.epoch, assignment.jobID, row.state as string, row.assignment_epoch as number, this.store.coordinatorEpoch]));
      }
    }
    if (!assignment && params.actionAvailable !== false && this.leases && this.actions) {
      let actionCursor: [number, string] | undefined;
      for (;;) {
        assertWorker();
        const rows = this.store.query(`SELECT a.id,a.request_json,w.stage,w.available_at,w.id work_id,t.capacity_id,t.capacity_version FROM work_items w JOIN actions a ON a.id=w.subject_id
          LEFT JOIN action_controls c ON c.action_id=a.id AND c.revision=a.revision
          JOIN action_auth_targets t ON t.action_id=a.id JOIN node_definitions d ON d.incarnation=? AND d.kind='capacity' AND d.id=t.capacity_id AND d.version=t.capacity_version
          JOIN runs r ON r.id=a.run_id LEFT JOIN business_leases l ON l.kind='action' AND l.subject_id=a.id
          WHERE w.kind='action' AND ((w.stage='recheck' AND CASE WHEN c.control_json IS NULL THEN json_extract(a.snapshot_json,'$.recheckRequested') ELSE json_extract(c.control_json,'$.recheckRequested') END=1)
            OR (w.stage='running' AND CASE WHEN c.control_json IS NULL THEN json_extract(a.snapshot_json,'$.latestAttempt.mode') ELSE json_extract(c.control_json,'$.latestAttempt.mode') END='recheck-only')
            OR (w.stage IN ('accepted','running') AND a.outcome_json IS NULL AND a.business_cancelled=0 AND a.business_terminal=0
            AND r.business_cancelled=0 AND r.business_terminal=0 AND CASE WHEN c.control_json IS NULL THEN json_extract(a.snapshot_json,'$.blocked') ELSE json_extract(c.control_json,'$.blockedCode') END IS NULL))
            AND (l.subject_id IS NULL OR l.released=1 OR l.expires_at<=?)${actionCursor ? " AND (w.available_at,w.id)>(?,?)" : ""}
          ORDER BY w.available_at,w.id LIMIT 1`, [params.incarnation, now, ...(actionCursor ?? [])]);
        for (const row of rows) {
          const control = readActionControl(this.store, row.id as string);
          if (control.kind !== "capacity" || control.target.id !== row.capacity_id || control.target.version !== row.capacity_version) throw harnessError("INTERNAL_ERROR", "Action control does not match its fixed definition");
          const recheck = row.stage === "recheck" && control.recheckRequested || row.stage === "running" && control.latestAttempt?.mode === "recheck-only";
          if (!recheck && (control.outcomeKind !== null || control.cancelRequested || control.blockedCode !== null)) continue;
          if (recheck) {
            const sessionConfig = parseJson(session.configuration_json as string) as unknown as { definitions: DefinitionDescriptor[] };
            if (!sessionConfig.definitions.some(definition => definition.kind === "capacity" && definition.ref.id === row.capacity_id && definition.ref.version === row.capacity_version && definition.supportsReconcile === true)) continue;
          }
          if (!recheck && this.store.effectiveCancellation({ kind: "action", id: row.id as string })) continue;
          const sampledAt = this.now();
          const preparedLease = this.leases.prepareClaim({ kind: "action", subjectID: row.id as string, nodeID: params.nodeID, incarnation: params.incarnation, workerPrincipalID: principalID, claimRequestID: params.claimRequestID }, recheck);
          if (!preparedLease.grant) continue;
          let prepared: ReturnType<ActionService["prepareClaim"]>;
          try {
            this.checkActionIdentity(preparedLease.grant, params, principalID);
            prepared = this.actions.prepareClaim(preparedLease.grant);
          } catch (error) {
            if (error && typeof error === "object" && "info" in error && ["FORBIDDEN", "EXECUTION_FENCED"].includes((error.info as { code?: string }).code ?? "")) continue;
            throw error;
          }
          const actionRequest = validatePreparedActionRequest(prepared.request);
          if (!fixedActionRequestMatches(this.store, row.id as string, actionRequest, row.request_json) || actionRequest.target.id !== row.capacity_id || actionRequest.target.version !== row.capacity_version
              || prepared.attemptID !== preparedLease.grant.attemptID || prepared.attemptNumber !== preparedLease.grant.attemptNumber) throw harnessError("INTERNAL_ERROR", "Action claim is not bound to its persisted request");
          assignment = { action: true, runID: prepared.runID, rootID: prepared.rootID, actionID: row.id as string, principalID: prepared.principalID,
            request: actionRequest, checkpoint: prepared.checkpoint, mode: prepared.mode, recovering: prepared.recovering, stage: prepared.stage,
            attemptID: prepared.attemptID, attemptNumber: prepared.attemptNumber, assignmentID: prepared.attemptID, nodeID: params.nodeID, incarnation: params.incarnation,
            lease: preparedLease.grant, leaseTTLms: preparedLease.grant.expiresAt - sampledAt, reportValueLimit: prepared.reportValueLimit };
          const storedRequest = parseJson(row.request_json as string) as Record<string, Json>;
          const storedCheckpoint = encodeCheckpoint(prepared.checkpoint);
          if (Object.hasOwn(storedRequest, "valueSource") || Object.hasOwn(storedRequest, "policySource") || Object.hasOwn(storedCheckpoint as object, "valueSource")) persistedAssignment = json({ encoding: "action-assignment-v1", assignment: { ...assignment, request: storedRequest, checkpoint: storedCheckpoint } });
          writes.push(...preparedLease.writes, ...prepared.writes); events.push(...prepared.events);
          leaseDeadline = preparedLease.validBefore;
          break;
        }
        if (assignment || rows.length === 0) break;
        const last = rows.at(-1)!;
        actionCursor = [last.available_at as number, last.work_id as string];
        await new Promise<void>(resolve => setImmediate(resolve));
        assertWorker(); this.store.assertAuthority();
        now = this.now(); session = this.session(params, principalID, now); assertClaimGate();
        writes[0] = this.sessionGuard(params, principalID, session.expires_at as number, now);
        const replayed = this.replay(operationID, "validation.claimed", params.nodeID, request);
        if (replayed) {
          const result = replayed as unknown as { assignment: NodeAssignment | null };
          return verifyReplay(result);
        }
      }
    }
    assertWorker();
    const claimed = this.commit(operationID, "validation.claimed", params.nodeID, request, writes, { assignment: persistedAssignment ?? assignment },
      Math.min(session.expires_at as number, assignment && !("action" in assignment) ? assignment.deadlineAt : Number.MAX_SAFE_INTEGER,
        leaseDeadline ?? Number.MAX_SAFE_INTEGER, assignment && !("driver" in assignment) && !("finalize" in assignment) && !("start" in assignment) && !("wake" in assignment) && !("action" in assignment) ? assignment.roundDeadlineAt : Number.MAX_SAFE_INTEGER), events) as unknown as { assignment: NodeAssignment | null };
    return verifyReplay(claimed);
  }

  private report(params: NodeRPCMap["report"]["params"], principalID: string, assertWorker: () => void): NodeRPCMap["report"]["result"] | Promise<NodeRPCMap["report"]["result"]> {
    const { assignment, conclusion } = params;
    if ("action" in assignment) throw harnessError("INVALID_ARGUMENT", "Use the Action report endpoint");
    if ("wake" in assignment) return this.reportWake(params, principalID, assignment);
    if ("start" in assignment) return this.reportStart(params, principalID, assignment);
    if ("finalize" in assignment) return this.reportFinalize(params, principalID, assignment, assertWorker);
    if ("driver" in assignment) return this.reportDriver(params, principalID, assignment, conclusion as DriverConclusion, assertWorker);
    return this.reportValidation(params, principalID, assignment, conclusion as ValidationConclusion, assertWorker);
  }

  private async reportValidation(params: NodeRPCMap["report"]["params"], principalID: string, assignment: ValidationAssignment, conclusion: ValidationConclusion, assertWorker: () => void): Promise<NodeRPCMap["report"]["result"]> {
    if (params.grant) throw harnessError("INVALID_ARGUMENT", "Validation reports cannot carry business grants");
    if (!("valid" in conclusion)) throw harnessError("INVALID_ARGUMENT", "Invalid validation conclusion");
    const operationID = identity("node.report", [assignment.assignmentID]);
    const original = this.store.query("SELECT request_json,result_json FROM history WHERE operation_id=?", [operationID])[0];
    if (assignment.payload.phase === "rewrite") {
      if (conclusion.valid) {
        if (!Array.isArray(assignment.payload.value) || !Array.isArray(conclusion.rewritten) || conclusion.rewritten.length !== assignment.payload.value.length)
          throw harnessError("INVALID_ARGUMENT", "Rewrite report is not bound to its input");
        for (let index = 0; index < conclusion.rewritten.length; index++) {
          const before = validateActionRequest(assignment.payload.value[index]);
          const revised = validateActionRequest(conclusion.rewritten[index]);
          if (before.key !== revised.key || before.kind !== revised.kind) throw harnessError("INVALID_ARGUMENT", "Rewrite cannot change action identity");
          if (!original) { assertJsonValueSize(revised.input, this.maxValueBytes); assertJsonValueSize(revised.config, this.maxValueBytes); }
        }
      } else if (conclusion.rewritten !== undefined) throw harnessError("INVALID_ARGUMENT", "Invalid rewrite conclusion");
    } else if (conclusion.rewritten !== undefined) throw harnessError("INVALID_ARGUMENT", "Unexpected rewrite result");
    if (assignment.nodeID !== params.nodeID || assignment.incarnation !== params.incarnation) throw harnessError("FORBIDDEN", "Report is not associated with this node");
    const owner = this.store.query("SELECT principal_id FROM node_sessions WHERE node_id=? AND incarnation=?", [params.nodeID, params.incarnation])[0];
    if (!owner || owner.principal_id !== principalID) throw harnessError("FORBIDDEN", "Node session belongs to a different worker");
    this.authorizePayload(assignment.payload);
    let replayPayload: string | undefined;
    const assertReplay = () => {
      assertWorker(); this.store.assertAuthority(); this.authorizePayload(assignment.payload); this.round(assignment.roundID, this.now());
      const row = this.job(assignment.jobID);
      if (replayPayload === undefined) { this.matchAssignment(assignment, row); replayPayload = row.payload_json as string; }
      if (row.state !== "accepted" || row.round_id !== assignment.roundID || row.assignment_id !== assignment.assignmentID || row.assignment_epoch !== assignment.assignmentEpoch
          || row.node_incarnation !== assignment.incarnation || row.payload_json !== replayPayload) throw harnessError("EXECUTION_FENCED", "This validation job is no longer applicable");
    };
    if (original) {
      assertReplay();
      const result = parseJson(original.result_json as string) as Record<string, Json>, source = validationValueSource(result.conclusion);
      const accepted = await readValidationConclusionAsync(this.store, result.conclusion, assertReplay);
      const left = canonicalJsonChunks(accepted)[Symbol.iterator](), right = canonicalJsonChunks(conclusion)[Symbol.iterator]();
      for (;;) {
        assertReplay(); const a = left.next(), b = right.next();
        if (a.done || b.done) { if (a.done !== b.done) throw harnessError("REQUEST_CONFLICT", "Validation report identity was reused"); break; }
        if (!Buffer.from(a.value).equals(b.value)) throw harnessError("REQUEST_CONFLICT", "Validation report identity was reused");
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      const request = validationReportRequest({ principalID, ...params, ...(source ? { conclusion: null, conclusionSource: source } : {}) }, original.request_json);
      this.store.commit({ operationID, kind: "validation.reported", subjectID: assignment.jobID, request, writes: [], evidence: null, result: null,
        ...(source ? { valuePublications: [source.value] } : {}) });
      assertReplay(); return { conclusion: accepted };
    }
    // Validate the complete fixed assignment once. Chunk boundaries recheck
    // its exact stored payload identity and mutable qualification metadata.
    this.currentAssignment(assignment, this.now());
    const expectedPayload = this.job(assignment.jobID).payload_json as string;
    const check = () => {
      assertWorker(); this.store.assertAuthority(); this.authorizePayload(assignment.payload); this.session(params, principalID, this.now()); this.round(assignment.roundID, this.now());
      const row = this.job(assignment.jobID);
      if (row.state !== "assigned" || row.round_id !== assignment.roundID || row.assignment_id !== assignment.assignmentID || row.assignment_epoch !== assignment.assignmentEpoch
          || row.node_incarnation !== assignment.incarnation || row.assignment_deadline_at !== assignment.deadlineAt || assignment.deadlineAt <= this.now() || row.payload_json !== expectedPayload)
        throw harnessError("EXECUTION_FENCED", "Validation assignment is no longer active");
      if (!this.supportsPreparation(params.incarnation, assignment.payload)) throw harnessError("EXECUTION_FENCED", "Fixed policies are unavailable on this node");
    };
    let source: ValidationValueSource | null = null;
    try {
    check(); source = await stageValidationValue(this.store, assignment, principalID, conclusion, this.valueQuota, check); check();
    const now = this.now(), session = this.session(params, principalID, now);
    const request = validationReportRequest({ principalID, ...params, ...(source ? { conclusion: null, conclusionSource: source } : {}) });
    this.store.commit({ operationID, kind: "validation.reported", subjectID: assignment.jobID, request, evidence: null, result: json({ conclusion: source ?? conclusion }),
      ...(source ? { valuePublications: [source.value] } : {}), validBefore: Math.min(session.expires_at as number, assignment.deadlineAt, assignment.roundDeadlineAt), writes: [
      this.sessionGuard(params, principalID, session.expires_at as number, now), this.roundGuard(assignment.roundID, now),
      write("UPDATE validation_jobs SET state=?,report_json=? WHERE id=? AND round_id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND assignment_deadline_at>?",
        ["accepted", { json: json(source ?? conclusion) }, assignment.jobID, assignment.roundID, "assigned", assignment.assignmentID, assignment.assignmentEpoch, assignment.incarnation, now]),
      ...validationPublicationWrites(source, this.store.coordinatorEpoch),
    ] });
    return { conclusion };
    } catch (error) {
      abandonValidationValue(this.store, source);
      if (error instanceof HarnessError && error.info.disposition === "rejected" && ["EXECUTION_FENCED", "REVISION_CONFLICT"].includes(error.info.code)
          && this.store.getCommit(operationID)) return this.reportValidation(params, principalID, assignment, conclusion, assertWorker);
      throw error;
    }
  }

  private supportsPreparation(incarnation: string, payload: ValidationPayload): boolean {
    if (payload.phase !== "rewrite" && payload.phase !== "admission" && payload.phase !== "report-output") return true;
    const context = payload.context;
    if (!context || typeof context !== "object" || Array.isArray(context) || context.domainID !== this.store.domainID || !Array.isArray(context.policies)) return false;
    if (payload.phase === "rewrite" && (payload.kind !== "agent" || context.policies.length === 0)) return false;
    if (payload.phase === "report-output" && payload.kind !== "capacity") return false;
    const installed = this.store.query("SELECT id,version FROM node_definitions WHERE incarnation=? AND kind='policy'", [incarnation]);
    return context.policies.every(policy => {
      if (!policy || typeof policy !== "object" || Array.isArray(policy) || !policy.ref || typeof policy.ref !== "object" || Array.isArray(policy.ref)) return false;
      const ref = policy.ref;
      return id(ref.id) && id(ref.version) && installed.some(row => row.id === ref.id && row.version === ref.version);
    });
  }

  private authorizeDriver(principalID: string, input: import("../contract/index.js").AdvanceInput): void {
    this.authorizePayload({ kind: "agent", ref: input.run.agent, phase: "input", value: input.run.input,
      config: input.run.config, principalID, operation: "client.submit", context: null });
  }
  private frameAssignmentValue(assignment: DriverAssignment | FinalizeAssignment, frame: Record<string, unknown>): Json {
    if (frame.input_encoding === "inline" && !hasChildFrameSource(frame.input_json)) return json(assignment);
    return makeFrameAssignmentReference(assignment, frame);
  }
  private frameAssignmentMatches(assignment: DriverAssignment | FinalizeAssignment, frame: Record<string, unknown>, job: Record<string, unknown>): boolean {
    const encoding = frame.input_encoding === "inline" ? "inline" : frame.input_encoding === "wait-ref-v1" ? "frame-ref-v1" : null;
    return encoding !== null && job.assignment_encoding === encoding && job.assignment_json === canonicalJson(this.frameAssignmentValue(assignment, frame));
  }
  private async readClaimResult(value: unknown): Promise<{ assignment: NodeAssignment | null }> {
    if (!value || typeof value !== "object" || Array.isArray(value) || !("assignment" in value)) throw harnessError("INTERNAL_ERROR", "Invalid claim receipt");
    const raw = (value as { assignment: unknown }).assignment;
    if (raw && typeof raw === "object" && "encoding" in raw && raw.encoding === "action-assignment-v1") {
      const metadata = (raw as { assignment?: ActionAssignment }).assignment;
      if (!metadata || metadata.action !== true || typeof metadata.actionID !== "string") throw harnessError("INTERNAL_ERROR", "Invalid Action assignment receipt");
      // Fence and authorize the original lease before hydrating a large value.
      this.checkReplayedAssignmentCurrent(metadata, this.now());
      const row = this.store.query("SELECT run_id,request_json FROM actions WHERE id=?", [metadata.actionID])[0];
      if (!row || row.run_id !== metadata.runID || canonicalJson(metadata.request) !== row.request_json) throw harnessError("INTERNAL_ERROR", "Action assignment source mismatch");
      const request = await readFixedActionRequestAsync(this.store, metadata.actionID, row.request_json);
      this.checkReplayedAssignmentCurrent(metadata, this.now());
      const checkpoint = await readCheckpointValueAsync(this.store, metadata.actionID, metadata.checkpoint as unknown as Json, false, () => this.checkReplayedAssignmentCurrent(metadata, this.now()));
      this.checkReplayedAssignmentCurrent(metadata, this.now());
      return { assignment: { ...metadata, request, checkpoint } };
    }
    if (!raw || typeof raw !== "object" || Array.isArray(raw) || !("encoding" in raw) || !["frame-assignment-v1", "child-frame-assignment-v1"].includes(raw.encoding as string))
      return value as { assignment: NodeAssignment | null };
    if (!("assignment" in raw) || !raw.assignment || typeof raw.assignment !== "object" || Array.isArray(raw.assignment))
      throw harnessError("INTERNAL_ERROR", "Invalid frame assignment receipt");
    const metadata = raw.assignment as Record<string, unknown>;
    const table = metadata.driver === true ? "driver_jobs" : metadata.finalize === true ? "finalize_jobs" : null;
    if (!table || typeof metadata.jobID !== "string" || typeof metadata.runID !== "string") throw harnessError("INTERNAL_ERROR", "Invalid frame assignment receipt");
    const row = this.store.query(`SELECT * FROM ${table} WHERE id=? AND run_id=?`, [metadata.jobID, metadata.runID])[0];
    if (!row || row.assignment_encoding !== (raw.encoding === "child-frame-assignment-v1" ? "inline" : "frame-ref-v1") || row.assignment_json !== canonicalJson(raw))
      throw harnessError("EXECUTION_FENCED", "Claim receipt assignment is no longer current");
    return { assignment: await readFrameAssignment(this.store, row) };
  }
  private async checkReplayedAssignment(assignment: NodeAssignment, now: number): Promise<void> {
    if ("finalize" in assignment) await this.matchFinalize(assignment, now, false);
    else if ("driver" in assignment) await this.matchDriver(assignment, now, false);
    this.checkReplayedAssignmentCurrent(assignment, this.now());
  }
  private checkReplayedAssignmentCurrent(assignment: NodeAssignment, now: number): void {
    if ("action" in assignment) {
      if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable");
      this.checkActionIdentity(assignment.lease, assignment, assignment.lease.workerPrincipalID);
      this.leases.assert(assignment.lease);
      const work = this.store.query("SELECT stage FROM work_items WHERE kind='action' AND subject_id=?", [assignment.actionID])[0];
      if (!work || work.stage !== "running" || assignment.assignmentID !== assignment.attemptID || assignment.lease.attemptID !== assignment.attemptID || assignment.lease.subjectID !== assignment.actionID) throw harnessError("EXECUTION_FENCED", "Action claim is no longer applicable");
      return;
    }
    if ("wake" in assignment) { this.matchWake(assignment); if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable"); this.leases.assert(assignment.lease); return; }
    if ("start" in assignment) { this.matchStart(assignment); if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable"); this.leases.assert(assignment.lease); return; }
    if ("finalize" in assignment) {
      const row = this.store.query("SELECT * FROM finalize_jobs WHERE id=?", [assignment.jobID])[0];
      const frame = this.store.query("SELECT id,run_id,state,input_json,input_encoding,input_operation_id FROM frames WHERE id=? AND run_id=?", [assignment.jobID, assignment.runID])[0];
      const work = this.store.query("SELECT stage FROM work_items WHERE kind='run' AND subject_id=?", [assignment.runID])[0];
      if (!row || !frame || frame.state !== "open" || !frameJobInput(row, frame) || row.state !== "assigned" || row.assignment_id !== assignment.assignmentID
          || row.assignment_epoch !== assignment.assignmentEpoch || row.node_incarnation !== assignment.incarnation || row.coordinator_epoch !== this.store.coordinatorEpoch
          || !this.frameAssignmentMatches(assignment, frame, row) || !this.canDrive(assignment.runID) || work?.stage !== "driver" && work?.stage !== "finalize")
        throw harnessError("EXECUTION_FENCED", "Finalize assignment is no longer active");
      this.authorizeDriver(assignment.principalID, assignment.input);
      if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable");
      this.leases.assert(assignment.lease);
      return;
    }
    if ("driver" in assignment) {
      this.authorizeDriver(assignment.principalID, assignment.input);
      const row = this.store.query("SELECT * FROM driver_jobs WHERE id=?", [assignment.jobID])[0];
      const frame = this.store.query("SELECT id,run_id,state,input_json,input_encoding,input_operation_id FROM frames WHERE id=? AND run_id=?", [assignment.jobID, assignment.runID])[0];
      const work = this.store.query("SELECT stage FROM work_items WHERE kind='run' AND subject_id=?", [assignment.runID])[0];
      if (!row || !frame || frame.state !== "open" || !frameJobInput(row, frame) || row.state !== "assigned" || row.assignment_id !== assignment.assignmentID
          || row.assignment_epoch !== assignment.assignmentEpoch || row.node_incarnation !== assignment.incarnation || row.coordinator_epoch !== this.store.coordinatorEpoch
          || !this.frameAssignmentMatches(assignment, frame, row) || !this.canDrive(assignment.runID) || work?.stage !== "driver")
        throw harnessError("EXECUTION_FENCED", "Driver assignment is no longer active");
      if (!this.leases) throw harnessError("STATE_CONFLICT", "Business leases are unavailable");
      this.leases.assert(assignment.lease);
    } else { this.authorizePayload(assignment.payload); this.currentAssignment(assignment, now); }
  }
  private frameGuard(assignment: DriverAssignment | FinalizeAssignment, frame: Record<string, unknown>): SQLWrite {
    if (frame.id !== assignment.jobID || frame.run_id !== assignment.runID || typeof frame.input_json !== "string"
        || !["inline", "wait-ref-v1"].includes(frame.input_encoding as string)
        || frame.input_encoding === "inline" && frame.input_operation_id !== null
        || frame.input_encoding === "wait-ref-v1" && typeof frame.input_operation_id !== "string")
      throw harnessError("EXECUTION_FENCED", "Frame input binding is unavailable");
    return write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=? AND input_json=? AND input_encoding=? AND input_operation_id IS ?",
      ["open", assignment.jobID, assignment.runID, "open", frame.input_json, frame.input_encoding as string, frame.input_operation_id as string | null]);
  }
  private driverGuards(assignment: DriverAssignment, frame: Record<string, unknown>): SQLWrite[] {
    return [
      this.frameGuard(assignment, frame),
      write("UPDATE work_items SET stage=? WHERE subject_id=? AND kind=? AND stage=?", ["driver", assignment.runID, "run", "driver"]),
    ];
  }
  private async matchDriver(assignment: DriverAssignment, now: number, accepted: boolean): Promise<Record<string, unknown>> {
    const row = this.store.query("SELECT * FROM driver_jobs WHERE id=?", [assignment.jobID])[0];
    if (!row || (!accepted && (!this.canDrive(assignment.runID) || row.coordinator_epoch !== this.store.coordinatorEpoch)) || row.state !== (accepted ? "accepted" : "assigned") || row.assignment_id !== assignment.assignmentID
        || row.assignment_epoch !== assignment.assignmentEpoch || row.node_incarnation !== assignment.incarnation
        ) throw harnessError("EXECUTION_FENCED", "Driver assignment is no longer active");
    const frame = this.store.query("SELECT id,run_id,state,input_json,input_encoding,input_operation_id FROM frames WHERE id=? AND run_id=?", [assignment.jobID, assignment.runID])[0];
    const inputJSON = frame && frameJobInput(row, frame);
    if (!inputJSON) throw harnessError("EXECUTION_FENCED", "Driver frame input is unavailable");
    const input = await readFrameInput(this.store, frame!);
    if (!await sameFrameAssignment(input, assignment.input)) throw harnessError("REQUEST_CONFLICT", "Driver assignment is bound to different frame input");
    this.store.assertAuthority();
    const current = this.store.query("SELECT * FROM driver_jobs WHERE id=?", [assignment.jobID])[0];
    if (!current || current.state !== row.state || current.assignment_id !== row.assignment_id || current.assignment_epoch !== row.assignment_epoch
        || current.node_incarnation !== row.node_incarnation || current.coordinator_epoch !== row.coordinator_epoch
        || current.assignment_json !== row.assignment_json) throw harnessError("EXECUTION_FENCED", "Driver assignment changed while reading its input");
    this.authorizeDriver(assignment.principalID, assignment.input);
    const work = this.store.query("SELECT stage FROM work_items WHERE subject_id=? AND kind=?", [assignment.runID, "run"])[0];
    if (!accepted && (!frame || frame.state !== "open" || work?.stage !== "driver" || !this.canDrive(assignment.runID))) {
      throw harnessError("EXECUTION_FENCED", "Driver frame is no longer applicable");
    }
    if (input.frameID !== assignment.jobID || input.run.id !== assignment.runID || input.run.agent.id !== row.agent_id
        || input.run.agent.version !== row.agent_version || !this.frameAssignmentMatches(assignment, frame!, row)) {
      throw harnessError("REQUEST_CONFLICT", "Driver assignment is bound to different content");
    }
    return frame!;
  }
  private async reportDriver(params: NodeRPCMap["report"]["params"], principalID: string, assignment: DriverAssignment, conclusion: DriverConclusion, assertWorker: () => void): Promise<NodeRPCMap["report"]["result"]> {
    if (!("ok" in conclusion)) throw harnessError("INVALID_ARGUMENT", "Invalid driver conclusion");
    if (assignment.nodeID !== params.nodeID || assignment.incarnation !== params.incarnation) throw harnessError("FORBIDDEN", "Report is not associated with this node");
    const owner = this.store.query("SELECT principal_id FROM node_sessions WHERE node_id=? AND incarnation=?", [params.nodeID, params.incarnation])[0];
    if (!owner || owner.principal_id !== principalID) throw harnessError("FORBIDDEN", "Node session belongs to a different worker");
    this.authorizeDriver(assignment.principalID, assignment.input);
    const operationID = identity("node.driver.report", [assignment.assignmentID]);
    const known = this.store.getCommit(operationID);
    const frame = await this.matchDriver(assignment, this.now(), known !== null);
    assertWorker(); this.store.assertAuthority();
    const old = known && this.store.query("SELECT report_json FROM driver_jobs WHERE id=?", [assignment.jobID])[0];
    // Preserve legacy receipts on replay; never migrate an already accepted report.
    const source = known && !driverValueSource(old?.report_json) ? null
      : await stageDriverValue(this.store, assignment.jobID, assignment.assignmentID, operationID, conclusion, this.valueQuota, () => {
        assertWorker();
        if (!known) { this.session(params, principalID, this.now()); this.reportGrant(params.grant, assignment, principalID); }
      });
    assertWorker(); this.store.assertAuthority();
    const request = { principalID, ...params,
      ...((frame.input_encoding === "wait-ref-v1" || hasChildFrameSource(frame.input_json)) ? { assignment: this.frameAssignmentValue(assignment, frame) } : {}),
      ...(source ? { conclusion: null, conclusionSource: source } : {}) };
    const prior = this.replay(operationID, "driver.reported", assignment.jobID, request, source ? [source.value] : undefined);
    if (prior) return { conclusion };
    await this.matchDriver(assignment, this.now(), false);
    assertWorker(); this.store.assertAuthority();
    const now = this.now(), session = this.session(params, principalID, now);
    const grant = this.reportGrant(params.grant, assignment, principalID);
    this.store.commit({ operationID, kind: "driver.reported", subjectID: assignment.jobID, request: json(request), evidence: null,
      result: json({ conclusion: source ?? conclusion }), ...(source ? { valuePublications: [source.value] } : {}),
      validBefore: Math.min(session.expires_at as number, grant.expiresAt), writes: [
        this.sessionGuard(params, principalID, session.expires_at as number, now), ...this.driverGuards(assignment, frame), ...this.leases!.guard(grant),
        write("UPDATE driver_jobs SET state=?,report_json=?,grant_json=? WHERE id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND coordinator_epoch=?",
          ["accepted", { json: json(source ?? conclusion) }, { json: json(grant) }, assignment.jobID, "assigned", assignment.assignmentID, assignment.assignmentEpoch, assignment.incarnation, this.store.coordinatorEpoch]),
        ...(source && this.store.query("SELECT value_id FROM driver_value_staging WHERE value_id=?", [source.value.id])[0]
          ? [write("DELETE FROM driver_value_staging WHERE value_id=? AND frame_id=? AND assignment_id=?", [source.value.id, assignment.jobID, assignment.assignmentID])] : []),
      ] });
    return { conclusion };
  }

  private reportGrant(grant: BusinessLeaseGrant | undefined, assignment: DriverAssignment | FinalizeAssignment | StartAssignment | WakeAssignment, principalID: string): BusinessLeaseGrant {
    if (!this.leases || !grant || grant.kind !== "run" || grant.subjectID !== assignment.runID || grant.nodeID !== assignment.nodeID
      || grant.workerPrincipalID !== principalID || grant.incarnation !== assignment.incarnation || grant.epoch !== assignment.lease.epoch
      || grant.attemptID !== null || grant.attemptNumber !== null) throw harnessError("EXECUTION_FENCED", "Report has no current business lease");
    this.leases.assert(grant);
    return grant;
  }
  private authorizeStart(assignment: Pick<StartAssignment, "principalID" | "agent">): void {
    this.authorizePayload({ kind: "agent", ref: assignment.agent, phase: "input", value: null, config: null,
      principalID: assignment.principalID, operation: "client.submit", context: null });
  }
  private matchStart(assignment: StartAssignment): void {
    const row = this.store.query("SELECT * FROM start_jobs WHERE id=?", [assignment.jobID])[0];
    if (!row || row.state !== "assigned" || row.assignment_id !== assignment.assignmentID || row.assignment_epoch !== assignment.assignmentEpoch
      || row.node_incarnation !== assignment.incarnation || row.coordinator_epoch !== this.store.coordinatorEpoch
      || row.assignment_json !== canonicalJson(assignment) || assignment.jobID !== assignment.runID)
      throw harnessError("EXECUTION_FENCED", "Start assignment is no longer active");
    this.authorizeStart(assignment);
  }
  private reportStart(params: NodeRPCMap["report"]["params"], principalID: string, assignment: StartAssignment): NodeRPCMap["report"]["result"] {
    if (!("ok" in params.conclusion) || params.conclusion.ok !== true || !("started" in params.conclusion) || params.conclusion.started !== true)
      throw harnessError("INVALID_ARGUMENT", "Invalid start acknowledgement");
    if (assignment.nodeID !== params.nodeID || assignment.incarnation !== params.incarnation) throw harnessError("FORBIDDEN", "Start is not associated with this node");
    const request = { principalID, ...params }, operationID = identity("node.start.report", [assignment.assignmentID]);
    const prior = this.replay(operationID, "start.reported", assignment.jobID, request);
    const now = this.now();
    if (prior) {
      this.authorizeStart(assignment);
      const owner = this.store.query("SELECT principal_id FROM node_sessions WHERE node_id=? AND incarnation=?", [params.nodeID, params.incarnation])[0];
      if (!owner || owner.principal_id !== principalID) throw harnessError("FORBIDDEN", "Node session belongs to a different worker");
      return prior as unknown as NodeRPCMap["report"]["result"];
    }
    const session = this.session(params, principalID, now);
    this.matchStart(assignment);
    const grant = this.reportGrant(params.grant, assignment, principalID);
    return this.commit(operationID, "start.reported", assignment.jobID, request, [
      this.sessionGuard(params, principalID, session.expires_at as number, now), ...this.leases!.guard(grant),
      write("UPDATE work_items SET stage=? WHERE kind=? AND subject_id=? AND stage=?", ["start", "run", assignment.runID, "start"]),
      write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND core_json IS NULL", [assignment.runID]),
      write("UPDATE start_jobs SET state=?,grant_json=? WHERE id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND coordinator_epoch=?", ["accepted", { json: json(grant) }, assignment.jobID, "assigned", assignment.assignmentID, assignment.assignmentEpoch, assignment.incarnation, this.store.coordinatorEpoch]),
    ], { conclusion: params.conclusion }, Math.min(session.expires_at as number, grant.expiresAt)) as unknown as NodeRPCMap["report"]["result"];
  }
  private matchWake(assignment: WakeAssignment): void {
    const row = this.store.query("SELECT * FROM wake_jobs WHERE id=? AND run_id=?", [assignment.waitID, assignment.runID])[0];
    const wait = this.store.query("SELECT winner,wake_json,resumed_frame_id FROM waits WHERE id=? AND run_id=?", [assignment.waitID, assignment.runID])[0];
    const work = this.store.query("SELECT stage FROM work_items WHERE kind='run' AND subject_id=?", [assignment.runID])[0];
    if (!row || row.state !== "assigned" || row.assignment_id !== assignment.assignmentID || row.assignment_epoch !== assignment.assignmentEpoch
      || row.node_incarnation !== assignment.incarnation || row.assignment_json !== canonicalJson(assignment) || assignment.jobID !== assignment.waitID
      || !wait || !wait.winner || !wait.wake_json || wait.resumed_frame_id !== null || work?.stage !== "wake" || !this.canWake(assignment.runID))
      throw harnessError("EXECUTION_FENCED", "Wake assignment is no longer active");
    this.authorizeStart(assignment);
  }
  private reportWake(params: NodeRPCMap["report"]["params"], principalID: string, assignment: WakeAssignment): NodeRPCMap["report"]["result"] {
    if (!("ok" in params.conclusion) || params.conclusion.ok !== true || !("woken" in params.conclusion) || params.conclusion.woken !== true)
      throw harnessError("INVALID_ARGUMENT", "Invalid wake acknowledgement");
    if (assignment.nodeID !== params.nodeID || assignment.incarnation !== params.incarnation) throw harnessError("FORBIDDEN", "Wake is not associated with this node");
    const request = { principalID, ...params }, operationID = identity("node.wake.report", [assignment.assignmentID]);
    const prior = this.replay(operationID, "wake.reported", assignment.jobID, request);
    const now = this.now();
    if (prior) {
      this.authorizeStart(assignment);
      const owner = this.store.query("SELECT principal_id FROM node_sessions WHERE node_id=? AND incarnation=?", [params.nodeID, params.incarnation])[0];
      if (!owner || owner.principal_id !== principalID) throw harnessError("FORBIDDEN", "Node session belongs to a different worker");
      return prior as unknown as NodeRPCMap["report"]["result"];
    }
    const session = this.session(params, principalID, now);
    this.matchWake(assignment);
    const grant = this.reportGrant(params.grant, assignment, principalID);
    const wait = this.store.query("SELECT winner,wake_json FROM waits WHERE id=? AND run_id=?", [assignment.waitID, assignment.runID])[0]!;
    const run = this.store.query("SELECT core_json FROM runs WHERE id=?", [assignment.runID])[0]!;
    return this.commit(operationID, "wake.reported", assignment.jobID, request, [
      this.sessionGuard(params, principalID, session.expires_at as number, now), ...this.leases!.guard(grant),
      write("UPDATE waits SET winner=? WHERE id=? AND run_id=? AND winner=? AND wake_json=? AND resumed_frame_id IS NULL", [wait.winner as string, assignment.waitID, assignment.runID, wait.winner as string, wait.wake_json as string]),
      write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND core_json=?", [assignment.runID, run.core_json as string]),
      write("UPDATE work_items SET stage=? WHERE kind=? AND subject_id=? AND stage=?", ["wake", "run", assignment.runID, "wake"]),
      write("UPDATE wake_jobs SET state=?,grant_json=? WHERE id=? AND run_id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND lease_epoch=?", ["accepted", { json: json(grant) }, assignment.waitID, assignment.runID, "assigned", assignment.assignmentID, assignment.assignmentEpoch, assignment.incarnation, grant.epoch]),
    ], { conclusion: params.conclusion }, Math.min(session.expires_at as number, grant.expiresAt)) as unknown as NodeRPCMap["report"]["result"];
  }
  private async matchFinalize(assignment: FinalizeAssignment, now: number, accepted: boolean): Promise<Record<string, unknown>> {
    const row = this.store.query("SELECT * FROM finalize_jobs WHERE id=?", [assignment.jobID])[0];
    if (!row || row.state !== (accepted ? "accepted" : "assigned") || row.assignment_id !== assignment.assignmentID
      || row.assignment_epoch !== assignment.assignmentEpoch || row.node_incarnation !== assignment.incarnation
      || row.coordinator_epoch !== this.store.coordinatorEpoch
      || (!accepted && !this.canDrive(assignment.runID))) throw harnessError("EXECUTION_FENCED", "Finalize assignment is no longer active");
    const frame = this.store.query("SELECT id,run_id,state,input_json,input_encoding,input_operation_id FROM frames WHERE id=? AND run_id=?", [assignment.jobID, assignment.runID])[0];
    if (!frame || !frameJobInput(row, frame) || !this.frameAssignmentMatches(assignment, frame, row)) throw harnessError("EXECUTION_FENCED", "Finalize frame input is unavailable");
    const input = await readFrameInput(this.store, frame);
    if (!await sameFrameAssignment(input, assignment.input)) throw harnessError("REQUEST_CONFLICT", "Finalize assignment is bound to different frame input");
    this.store.assertAuthority();
    const current = this.store.query("SELECT * FROM finalize_jobs WHERE id=?", [assignment.jobID])[0];
    if (!current || current.state !== row.state || current.assignment_id !== row.assignment_id || current.assignment_epoch !== row.assignment_epoch
        || current.node_incarnation !== row.node_incarnation || current.coordinator_epoch !== row.coordinator_epoch
        || current.assignment_json !== row.assignment_json || !accepted && !this.canDrive(assignment.runID))
      throw harnessError("EXECUTION_FENCED", "Finalize assignment changed while reading its input");
    this.authorizeDriver(assignment.principalID, assignment.input);
    return frame;
  }
  private async reportFinalize(params: NodeRPCMap["report"]["params"], principalID: string, assignment: FinalizeAssignment, assertWorker: () => void): Promise<NodeRPCMap["report"]["result"]> {
    if (!("ok" in params.conclusion) || params.conclusion.ok !== true || !("finalized" in params.conclusion) || params.conclusion.finalized !== true) throw harnessError("INVALID_ARGUMENT", "Invalid finalize acknowledgement");
    if (assignment.nodeID !== params.nodeID || assignment.incarnation !== params.incarnation) throw harnessError("FORBIDDEN", "Finalize is not associated with this node");
    const operationID = identity("node.finalize.report", [assignment.assignmentID]);
    const storedFrame = this.store.query("SELECT id,run_id,state,input_json,input_encoding,input_operation_id FROM frames WHERE id=? AND run_id=?", [assignment.jobID, assignment.runID])[0];
    let request: unknown = { principalID, ...params };
    if (storedFrame && (storedFrame.input_encoding === "wait-ref-v1" || hasChildFrameSource(storedFrame.input_json))) {
      const originalInput = await readFrameInput(this.store, storedFrame);
      if (!await sameFrameAssignment(originalInput, assignment.input)) throw harnessError("REQUEST_CONFLICT", "Finalize assignment is bound to different frame input");
      assertWorker(); this.store.assertAuthority(); this.authorizeDriver(assignment.principalID, assignment.input);
      request = { principalID, ...params, assignment: this.frameAssignmentValue(assignment, storedFrame) };
    }
    const prior = this.replay(operationID, "finalize.reported", assignment.jobID, request);
    if (prior) {
      // The same frame can later receive another finalization assignment. A
      // durable acknowledgement remains replayable by its original worker.
      this.authorizeDriver(assignment.principalID, assignment.input);
      const owner = this.store.query("SELECT principal_id FROM node_sessions WHERE node_id=? AND incarnation=?", [params.nodeID, params.incarnation])[0];
      if (!owner || owner.principal_id !== principalID) throw harnessError("FORBIDDEN", "Node session belongs to a different worker");
      return prior as unknown as NodeRPCMap["report"]["result"];
    }
    const frame = await this.matchFinalize(assignment, this.now(), false);
    assertWorker(); this.store.assertAuthority();
    const now = this.now(), session = this.session(params, principalID, now);
    const grant = this.reportGrant(params.grant, assignment, principalID);
    const work = this.store.query("SELECT stage FROM work_items WHERE kind='run' AND subject_id=? AND stage IN ('driver','finalize')", [assignment.runID])[0];
    if (!work) throw harnessError("EXECUTION_FENCED", "Finalize work is no longer current");
    return this.commit(operationID, "finalize.reported", assignment.jobID, request, [
      this.sessionGuard(params, principalID, session.expires_at as number, now), ...this.leases!.guard(grant),
      this.frameGuard(assignment, frame),
      write("UPDATE work_items SET stage=? WHERE kind=? AND subject_id=? AND stage=?", [work.stage as string, "run", assignment.runID, work.stage as string]),
      write("UPDATE finalize_jobs SET state=?,grant_json=? WHERE id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND coordinator_epoch=?", ["accepted", { json: json(grant) }, assignment.jobID, "assigned", assignment.assignmentID, assignment.assignmentEpoch, assignment.incarnation, this.store.coordinatorEpoch]),
    ], { conclusion: params.conclusion }, Math.min(session.expires_at as number, grant.expiresAt)) as unknown as NodeRPCMap["report"]["result"];
  }

  private unregister(params: NodeRPCMap["unregister"]["params"], principalID: string): { stopped: true } {
    const request = { principalID, ...params };
    const operationID = identity("node.unregister", [principalID, params.incarnation]);
    const prior = this.replay(operationID, "node.unregistered", params.nodeID, request);
    if (prior) return prior as unknown as { stopped: true };
    const session = this.store.query("SELECT node_id,principal_id FROM node_sessions WHERE incarnation=?", [params.incarnation])[0];
    if (session && (session.node_id !== params.nodeID || session.principal_id !== principalID)) throw harnessError("FORBIDDEN", "Node session belongs to a different worker");
    const now = this.now();
    // Close may overtake an already-sent register request. Retain that decision
    // so the delayed request cannot create a new online session after cleanup.
    const writes = session
      ? [write("UPDATE node_sessions SET state=? WHERE node_id=? AND incarnation=? AND principal_id=?", ["offline", params.nodeID, params.incarnation, principalID])]
      : [write("INSERT INTO node_sessions (incarnation,node_id,principal_id,state,expires_at,registered_at,configuration_json) VALUES (?,?,?,?,?,?,?)",
        [params.incarnation, params.nodeID, principalID, "offline", now, now, { json: { closedBeforeRegistration: true } }])];
    return this.commit(operationID, "node.unregistered", params.nodeID, request, writes, { stopped: true }) as { stopped: true };
  }

  private session(params: { nodeID: string; incarnation: string }, principalID: string, now: number) {
    const row = this.store.query("SELECT * FROM node_sessions WHERE node_id=? AND incarnation=?", [params.nodeID, params.incarnation])[0];
    if (!row || row.principal_id !== principalID) throw harnessError("FORBIDDEN", "Node session belongs to a different worker");
    if (row.state !== "online" || (row.expires_at as number) <= now) throw harnessError("EXECUTION_FENCED", "Node session is no longer active");
    return row;
  }
  private sessionGuard(params: { nodeID: string; incarnation: string }, principalID: string, expiresAt: number, now: number) {
    return write("UPDATE node_sessions SET expires_at=? WHERE node_id=? AND incarnation=? AND principal_id=? AND state=? AND expires_at=? AND expires_at>?",
      [expiresAt, params.nodeID, params.incarnation, principalID, "online", expiresAt, now]);
  }
  private round(roundID: string, now: number) {
    const row = this.store.query("SELECT * FROM prepare_rounds WHERE id=?", [roundID])[0];
    if (!row || row.state !== "pending" || (row.deadline_at as number) <= now) throw harnessError("EXECUTION_FENCED", "Preparation round is no longer applicable");
    return row;
  }
  private roundGuard(roundID: string, now: number) {
    return write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", roundID, "pending", now]);
  }
  private job(jobID: string) {
    const row = this.store.query("SELECT * FROM validation_jobs WHERE id=?", [jobID])[0];
    if (!row) throw harnessError("EXECUTION_FENCED", "Validation job is no longer applicable");
    return row;
  }
  private currentAssignment(assignment: ValidationAssignment, now: number): void {
    this.round(assignment.roundID, now);
    const row = this.job(assignment.jobID);
    this.matchAssignment(assignment, row);
    if (row.state !== "assigned" || assignment.deadlineAt <= now) throw harnessError("EXECUTION_FENCED", "Validation assignment is no longer active");
  }
  private matchAssignment(assignment: ValidationAssignment, row: ReturnType<NodeService["job"]>): void {
    if (row.assignment_id !== assignment.assignmentID || row.node_incarnation !== assignment.incarnation || row.assignment_epoch !== assignment.assignmentEpoch) {
      throw harnessError("EXECUTION_FENCED", "A newer assignment owns this validation job");
    }
    const round = this.store.query("SELECT deadline_at,principal_id FROM prepare_rounds WHERE id=?", [row.round_id!])[0]!;
    const payload = validateValidationPayload(parseJson(row.payload_json as string));
    const expected: ValidationAssignment = { jobID: row.id as string, roundID: row.round_id as string, payload,
      assignmentID: row.assignment_id as string, assignmentEpoch: row.assignment_epoch as number,
      nodeID: this.store.query("SELECT node_id FROM node_sessions WHERE incarnation=?", [row.node_incarnation!])[0]!.node_id as string,
      incarnation: row.node_incarnation as string, deadlineAt: row.assignment_deadline_at as number, roundDeadlineAt: round.deadline_at as number };
    const left = canonicalJsonChunks(expected)[Symbol.iterator](), right = canonicalJsonChunks(assignment)[Symbol.iterator]();
    let same = true;
    for (;;) { const a = left.next(), b = right.next(); if (a.done || b.done) { same = a.done === b.done; break; } if (!Buffer.from(a.value).equals(b.value)) { same = false; break; } }
    if (payload.principalID !== round.principal_id || !same) throw harnessError("REQUEST_CONFLICT", "Assignment is bound to different content");
  }
  private jobs(inputs: ValidationJobInput[], principalID: string): ValidationJobInput[] {
    if (!Array.isArray(inputs) || inputs.length === 0 || inputs.length > 256) throw harnessError("INVALID_ARGUMENT", "A preparation batch needs 1..256 jobs");
    const seen = new Set<string>();
    return inputs.map(input => {
      if (!id(input.jobID) || seen.has(input.jobID)) throw harnessError("REQUEST_CONFLICT", "Duplicate or invalid validation job identity");
      seen.add(input.jobID);
      const payload = validateValidationPayload(input.payload);
      if (payload.principalID !== principalID) throw harnessError("FORBIDDEN", "Validation subject must match its round");
      this.authorizePayload(payload);
      return { jobID: input.jobID, payload };
    });
  }
  private insertJob(roundID: string, job: ValidationJobInput): SQLWrite {
    return write("INSERT INTO validation_jobs (id,round_id,payload_json,state) VALUES (?,?,?,?)", [job.jobID, roundID, { json: json(job.payload) }, "pending"]);
  }
  private nodeSession(params: { nodeID: string; incarnation: string }, serverTime: number, expiresAt: number): NodeSession {
    return { nodeID: params.nodeID, incarnation: params.incarnation, expiresAt, serverTime,
      heartbeatMs: this.options.heartbeatMs, pollMs: this.options.pollMs };
  }
  private now(): number {
    const now = this.options.clock.now();
    if (!Number.isSafeInteger(now) || now < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock");
    return now;
  }
  private replay(operationID: string, kind: string, subjectID: string, request: unknown, valuePublications?: CommitBatch["valuePublications"]): Json | undefined {
    if (!this.store.getCommit(operationID)) return undefined;
    return this.store.commit({ operationID, kind, subjectID, request: json(request), writes: [], evidence: null, result: null,
      ...(valuePublications ? { valuePublications } : {}) }).result;
  }
  private commit(operationID: string, kind: string, subjectID: string, request: unknown, writes: SQLWrite[], result: unknown, validBefore?: number, events?: HistoryEvent[], absent?: CommitBatch["absent"]): Json {
    return this.store.commit({ operationID, kind, subjectID, request: json(request), writes, evidence: null, result: json(result), ...(validBefore === undefined ? {} : { validBefore }), ...(events?.length ? { events } : {}), ...(absent?.length ? { absent } : {}) }).result;
  }
}

function identity(kind: string, parts: string[]): string { return `${kind}:${canonicalJson(parts)}`; }
function add(a: number, b: number): number {
  const sum = a + b;
  if (!Number.isSafeInteger(sum) || sum < 0) throw harnessError("LIMIT_EXCEEDED", "Node/validation time or generation exhausted");
  return sum;
}
export function nodeServiceOptions(options: NodeServiceOptions = {}): Required<NodeServiceOptions> {
  if (!options || typeof options !== "object" || Array.isArray(options)
      || Object.keys(options).some(key => !["clock", "sessionTTLms", "assignmentTTLms", "heartbeatMs", "pollMs"].includes(key))) throw harnessError("INVALID_ARGUMENT", "Invalid node service configuration");
  const result = { clock: options.clock ?? { now: Date.now }, sessionTTLms: options.sessionTTLms ?? 15_000,
    assignmentTTLms: options.assignmentTTLms ?? 2_000, heartbeatMs: options.heartbeatMs ?? 1_000, pollMs: options.pollMs ?? 50 };
  if (!result.clock || typeof result.clock.now !== "function" || ![result.sessionTTLms, result.assignmentTTLms, result.heartbeatMs, result.pollMs].every(value => Number.isSafeInteger(value) && value > 0)
      || result.heartbeatMs >= result.sessionTTLms) throw harnessError("INVALID_ARGUMENT", "Node timeouts must be finite positive integers and heartbeat shorter than session TTL");
  return result;
}
