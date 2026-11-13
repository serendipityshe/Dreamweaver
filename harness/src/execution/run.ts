import { retireSubmitInputs } from "../persistence/index.js";
import { readValidationConclusion, readValidationConclusionAsync, retireValidationValues } from "../persistence/index.js";
import { initialWakeNeedsReference, makeInitialWaitWake } from "../persistence/index.js";
import { encodeRunInput, encodeRunFrame, childAcceptanceEvent, expectedRunSnapshot, continuationSource, retireCheckpointValues, retireNodePolicies, readRunControl, type RunControl, type ContinuationSource } from "../persistence/index.js";
import { stageActionRequestValues, retireActionRequestValues, encodeFixedActionRequest, requestValuePublicationWrites, fixedActionRequestMatches, type ValuePublication, type ActionRequestValue } from "../persistence/index.js";
import { readNodeConfiguration, storedPolicySource, readPreparationPolicies, encodePreparationPolicies } from "../persistence/index.js";
import { actionAcceptanceEvent, readActionAcceptance, expandActionRequests } from "../persistence/index.js";
import { encodePreparationOriginals, encodePreparationRewrites, encodePreparationResolved, readPreparationOriginals, readPreparationRewritten, readPreparationResolved } from "../persistence/index.js";
import { retireDriverValues, readDriverValue, readDriverValueAsync, readFrameDecision, driverDecisionStorage, dispatchDecisionEvent, completionEvents, readRunHistory, encodeRunSnapshot, readRunSnapshot, agentOutputValidation, completionRequest, dispatchRequest, readTransitionRequest, encodeActionSnapshot, readActionSnapshot } from "../persistence/index.js";
import { randomUUID } from "node:crypto";
import { createRunCore, restoreRunCore, type RunCore } from "../../../core/src/run-core.js";
import { completeSnapshot, resumeSnapshot, startSnapshot, suspendSnapshot } from "../../../core/src/transitions.js";
import type { StartedSnapshot, ResumedSnapshot } from "../../../core/src/types.js";
import { CoreError } from "../../../core/src/errors.js";
import { canonicalJson, canonicalJsonChunks, compactDispatchPayload, dispatchPayloadMatches, copyJson, harnessError, HarnessError, parseJson, validateDecision, validateActionRequest, type ActionRequest, type ActionView, type AppliedPolicy, type AdvanceInput, type Decision, type Fact, type Json, type Outcome, type PreparedActionRequest, type RunView, type ValidationPayload, type Wake } from "../contract/index.js";
import { cancellationGuard, canonicalValueWitness, frameJobInput, readActionReuseIdentity, readFactOutcome, readFactOutcomeForEvidence, readFrameAssignment, readFrameInput, readWaitWake, verifyDiagnosticTransferForEvidence, waitFactWitness, type SQLiteStore, type SQLWrite, type AbsenceGuard, type CommitBatch } from "../persistence/index.js";
import { BusinessLeaseService, type LeaseGrant } from "./lease.js";
import { resolvedEffectGuard } from "./action-effect.js";
import { prepareRemoteCompletion, verifyRemoteCompletionReferenced } from "./remote-completion.js";
import { hasWaitResolutionEvents, referencedWaitResolutionEvents } from "./wait-history.js";

type Row = ReturnType<SQLiteStore["query"]>[number];
function agentSubject(policy: AppliedPolicy): { id: string; version: string } | null {
  const config = policy.config;
  if (!config || typeof config !== "object" || Array.isArray(config) || Object.keys(config).length !== 1 || !Object.hasOwn(config, "dw.agent.subject")) return null;
  const marker = config["dw.agent.subject"];
  if (!marker || typeof marker !== "object" || Array.isArray(marker) || Object.keys(marker).length !== 2 || typeof marker.id !== "string" || typeof marker.version !== "string") return null;
  return { id: marker.id, version: marker.version };
}
function policiesForRun(policies: AppliedPolicy[], agent: { id: string; version: string }): { policies: AppliedPolicy[]; scoped: boolean } {
  if (!policies.some(policy => agentSubject(policy))) return { policies, scoped: false };
  const host = policies.filter(policy => !agentSubject(policy));
  const mine = policies.flatMap(policy => {
    const subject = agentSubject(policy);
    return subject?.id === agent.id && subject.version === agent.version ? [{ ref: policy.ref, config: null }] : [];
  });
  return { policies: [...host, ...mine], scoped: true };
}
const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
function admissionKey(row: Row): string | undefined {
  const payload = decode<{ context?: { original?: { key?: unknown } } }>(row.payload_json);
  const key = payload?.context?.original?.key;
  return typeof key === "string" ? key : undefined;
}
const sameJsonChunks = (left: unknown, right: unknown): boolean => {
  const a = canonicalJsonChunks(left)[Symbol.iterator](), b = canonicalJsonChunks(right)[Symbol.iterator]();
  for (;;) {
    const x = a.next(), y = b.next();
    if (x.done || y.done) return x.done === y.done;
    if (!Buffer.from(x.value).equals(Buffer.from(y.value))) return false;
  }
};

/** Check the canonical UTF-8 frame budget without assembling a full JSON
 * string. Yield between chunks so a large rejected frame does not monopolize
 * the coordinator while other work needs to renew its lease. */
async function frameExceedsBytes(value: unknown, limit: number): Promise<boolean> {
  let bytes = 0;
  for (const chunk of canonicalJsonChunks(value)) {
    bytes += chunk.byteLength;
    if (bytes > limit) return true;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  return false;
}

/** Coordinator-only execution, Core handoffs and evidence-bound recovery. */
export function createRunService(store: SQLiteStore, options: { clock: { now(): number }; pollMs: number; prepareTimeoutMs: number; retryAfterMs: number; maxFrameBytes: number; maxValueBytes: number; maxResponseBytes: number; enableReferencedFrameInput?: boolean; valueQuota?: number; authorizePrincipal(principalID: string, method: "client.submit", kind: "agent" | "capacity", ref: { id: string; version?: string }): void; authorizeWorker(grant: LeaseGrant): void }) {
  const cores = new Map<string, RunCore>();
  const quarantined = new Set<string>();
  const transitionsInFlight = new Set<string>();
  // A quarantined cold Run may have no live proposal. Track live Core unknowns
  // separately so an unrelated frozen store can recover without releasing one.
  const unknownCores = new Map<string, string | null>();
  let pausedNonCoreOperation: string | undefined;
  // Only locally sealed control steps can recover through this path. In
  // particular an intent's Core confirmation must have returned rejected and
  // released its control slot; a live unknown proposal is never bypassed.
  let sealedControl: { batch: CommitBatch; epoch: number; run: string; beforeWork: string; work: string; core: RunCore | undefined; snapshot: string; released: boolean } | undefined;
  let pausedControlOperation: string | undefined;
  let workWitness: number | undefined;
  // Taken immediately before a Core commit. Unrelated commits can move the
  // database witness; this preimage is the narrower proof that the target
  // operation itself did not land. A missing witness is never replaced by it.
  let recoveryScope: { runID: string; evidence: string } | undefined;
  // Captured only when a newly activated coordinator starts, before its own
  // scanner can produce an intent. Its epoch fences all prior commit paths.
  const coldControlIntents = new Map<string, string>();
  const coldControlStarts = new Set<string>();
  const leases = new BusinessLeaseService(store, { clock: options.clock, ttlMs: 1 });
  const workerAllowed = (grant: LeaseGrant) => { try { options.authorizeWorker(grant); return true; } catch { return false; } };
  let closed = false, scanning = false;
  let timer: ReturnType<typeof setInterval> | undefined;
  let scheduled: ReturnType<typeof setImmediate> | undefined;
  const now = () => options.clock.now();
  const runWrite = (view: RunView, previous: RunView, core?: Json, before?: Json, resultSource?: { frameID: string; operationID: string }, continuation?: ContinuationSource): SQLWrite => write(
    `UPDATE runs SET revision=?,frame_count=?,snapshot_json=?${core === undefined ? "" : ",core_json=?"} WHERE id=? AND revision=? AND snapshot_json=?${core === undefined ? "" : before === null ? " AND core_json IS NULL" : " AND core_json=?"}`,
    [view.revision, view.frameCount, { json: encodeRunInput(store, resultSource ? encodeRunSnapshot(view, resultSource.frameID, resultSource.operationID) : view, undefined, continuation) }, ...(core === undefined ? [] : [{ json: core }]), view.id, previous.revision, expectedRunSnapshot(store, previous), ...(core === undefined || before === null ? [] : [canonicalJson(before)])]);
  const stage = (id: string, from: string, to: string) => write("UPDATE work_items SET stage=? WHERE id=? AND stage=?", [to, id, from]);
  const releaseGrant = (grant: LeaseGrant) => write("UPDATE business_leases SET released=1 WHERE kind=? AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at>=? AND released=0 AND attempt_id IS ? AND attempt_number IS ?", [grant.kind, grant.subjectID, grant.nodeID, grant.workerPrincipalID, grant.incarnation, grant.epoch, grant.expiresAt, grant.attemptID, grant.attemptNumber]);
  const finalizeGuard = (job: Row, grant: LeaseGrant) => write("UPDATE finalize_jobs SET state=? WHERE id=? AND run_id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND assignment_json=? AND assignment_encoding=? AND grant_json=?", ["accepted", job.id as string, grant.subjectID, "accepted", job.assignment_id as string, job.assignment_epoch as number, grant.incarnation, job.assignment_json as string, job.assignment_encoding as string, job.grant_json as string]);
  async function finalizationGrant(job: Row | undefined, frame: Row, view: RunView, principalID: string): Promise<LeaseGrant | null> {
    if (!job || job.state !== "accepted" || !job.grant_json || !job.assignment_json) return null;
    const grant = decode<LeaseGrant>(job.grant_json);
    const assignment = await readFrameAssignment(store, job);
    if (!("finalize" in assignment)) return null;
    if (grant.kind !== "run" || grant.subjectID !== view.id || assignment.finalize !== true || assignment.jobID !== frame.id || assignment.runID !== view.id
        || assignment.principalID !== principalID || !sameJsonChunks(assignment.input, await readFrameInput(store, frame)) || assignment.nodeID !== grant.nodeID
        || assignment.incarnation !== grant.incarnation || assignment.assignmentID !== job.assignment_id || assignment.assignmentEpoch !== job.assignment_epoch
        || !assignment.lease || assignment.lease.epoch !== grant.epoch || assignment.lease.workerPrincipalID !== grant.workerPrincipalID
        || job.agent_id !== view.agent.id || job.agent_version !== view.agent.version || job.principal_id !== principalID
        || frameJobInput(job, frame) === null || job.node_incarnation !== grant.incarnation || job.coordinator_epoch !== store.coordinatorEpoch) return null;
    try { leases.assert(grant); } catch { return null; }
    return grant;
  }
  const insertFinalization = (frame: Row, view: RunView, principalID: string) => write("INSERT INTO finalize_jobs (id,run_id,agent_id,agent_version,principal_id,input_json,coordinator_epoch,state,input_encoding) VALUES (?,?,?,?,?,?,?,?,?)", [frame.id as string, view.id, view.agent.id, view.agent.version, principalID, { json: null }, store.coordinatorEpoch, "pending", "frame-ref-v1"]);
  const commit = (operationID: string, kind: string, runID: string, request: unknown, writes: SQLWrite[], evidence: unknown, events: { type: string; subjectID: string; data: Json }[] = [], validBefore?: number, absent?: AbsenceGuard[], frameFactSnapshot?: { frameID: string; factIDs: string[] }, factSnapshot?: { actionID: string; factID: string | null }[], valuePublications?: ValuePublication[]) => {
    if (kind.startsWith("core.")) noteRecoveryScope(runID);
    store.commit({ operationID, kind, subjectID: runID, request: json(request), writes: [cancellationGuard({ kind: "run", id: runID }), ...writes], evidence: json(evidence), result: null, events, ...(validBefore === undefined ? {} : { validBefore }), ...(absent ? { absent } : {}), ...(frameFactSnapshot ? { frameFactSnapshot } : {}), ...(factSnapshot ? { factSnapshot } : {}), ...(valuePublications?.length ? { valuePublications } : {}) });
  };
  const fixedFrameFacts = async (frame: Row, evidence = false): Promise<{ frameID: string; factIDs: string[] }> => {
    const input = await readFrameInput(store, frame, evidence);
    return { frameID: frame.id as string, factIDs: input.wake.kind === "start" ? [] : input.wake.facts.map(fact => fact.id) };
  };
  function relatedRecoveryEvidence(runID: string): string {
    const runs: Record<string, unknown>[] = [];
    const seen = new Set<string>();
    let current: string | null = runID;
    while (current && !seen.has(current)) {
      seen.add(current);
      const run = store.query("SELECT * FROM runs WHERE id=?", [current])[0] as Record<string, unknown> | undefined;
      if (!run) break;
      runs.push(run);
      const parentAction: unknown = run.parent_action_id;
      if (typeof parentAction !== "string") break;
      const parent = store.query("SELECT run_id FROM actions WHERE id=?", [parentAction])[0] as Record<string, unknown> | undefined;
      const parentRun: unknown = parent?.run_id;
      current = typeof parentRun === "string" ? parentRun : null;
    }
    const rows = (sql: string, params: (string | number)[]) => store.query(sql, params) as unknown;
    return canonicalJson({
      runs,
      frames: rows("SELECT * FROM frames WHERE run_id=? ORDER BY number, id", [runID]),
      leases: rows("SELECT * FROM business_leases WHERE (kind='run' AND subject_id=?) OR (kind='action' AND subject_id IN (SELECT id FROM actions WHERE run_id=?)) ORDER BY kind, subject_id", [runID, runID]),
      jobs: rows("SELECT * FROM driver_jobs WHERE run_id=? ORDER BY id", [runID]),
      work: rows("SELECT * FROM work_items WHERE (kind='run' AND subject_id=?) OR (kind='action' AND subject_id IN (SELECT id FROM actions WHERE run_id=?)) ORDER BY id", [runID, runID]),
      starts: rows("SELECT * FROM start_jobs WHERE id=?", [runID]),
    });
  }
  function noteRecoveryScope(runID: string): void {
    try { recoveryScope = { runID, evidence: relatedRecoveryEvidence(runID) }; }
    catch { recoveryScope = undefined; }
  }
  function recoveryScopeMatches(runID: string): boolean {
    if (!recoveryScope || recoveryScope.runID !== runID) return false;
    try { return relatedRecoveryEvidence(runID) === recoveryScope.evidence; }
    catch { return false; }
  }
  function transitionBefore(runID: string): string {
    const run = store.query("SELECT * FROM runs WHERE id=?", [runID])[0];
    if (!run) throw new Error("Run is unavailable before Core transition");
    return canonicalJson({ run, cancellation: store.effectiveCancellation({ kind: "run", id: runID }),
      work: store.query("SELECT * FROM work_items WHERE kind='run' AND subject_id=?", [runID]),
      frame: store.query("SELECT id FROM frames WHERE run_id=? LIMIT 1", [runID]),
      starts: store.query("SELECT * FROM start_jobs WHERE id=?", [runID]),
      lease: store.query("SELECT * FROM business_leases WHERE kind='run' AND subject_id=?", [runID]) });
  }
  async function transition(runID: string, core: RunCore, invoke: () => Promise<unknown>) {
    // The scan awaits each Core transition. Keep this global invariant explicit:
    // a storage reopen must never unfreeze while another proposal is in flight.
    if (transitionsInFlight.size) throw new Error("A Core control transition is already in flight");
    // The per-work witness covers the entire committed database. When later
    // unrelated commits move it, the pre-commit Run, frame and lease preimage
    // can still prove this operation did not land. Initial start also checks
    // its bounded local pre-state. A missing witness stays unresolved.
    const beforeCore = core.getSnapshot(), witness = workWitness;
    recoveryScope = undefined;
    const initialWork = beforeCore === null ? store.query("SELECT stage FROM work_items WHERE kind='run' AND subject_id=?", [runID])[0] : null;
    const previous = initialWork?.stage === "start" ? transitionBefore(runID) : null, originalEpoch = store.coordinatorEpoch;
    transitionsInFlight.add(runID);
    try { await invoke(); }
    catch (error) {
      if (error instanceof CoreError && error.code === "CONFIRMATION_UNKNOWN") {
        quarantined.add(runID);
        unknownCores.set(runID, error.operationID ?? null);
        let reopened = false, rejected = false;
        // Only an authoritative proof of the original operation can install its candidate.
        try {
          if (error.operationID) {
            const pending = store.unresolvedCommit;
            if (store.requiresRecovery) {
              const recovery = store.recoverAuthority();
              if (recovery.oldEpoch !== originalEpoch) throw new Error("Recovered authority does not match the sealed transition");
              reopened = true;
            }
            const proof = store.getCommit(error.operationID);
            if (proof && pending && (pending.operationID !== error.operationID || !store.matchesCommitEvidence(pending)))
              throw new Error("Recovered Core receipt does not match the original commit identity");
            const record = store.query("SELECT request_json FROM history WHERE operation_id=?", [error.operationID])[0];
            const persisted = store.query("SELECT snapshot_json,core_json FROM runs WHERE id=?", [runID])[0];
            if (proof && proof.subjectID === runID && proof.kind === "core.recovery_resume" && record && persisted) {
              const request = decode<{ proposal: { operationID: string; action: string; before: Json; after: Json }; frameID: string; coordinatorEpoch: number }>(record.request_json);
              const row = store.query("SELECT * FROM runs WHERE id=?", [runID])[0];
              const view = readRunSnapshot(store, persisted.snapshot_json);
              const recovered = row && await activeFrameEvidence(row, view, persisted.core_json as string, undefined, 0, true);
              if (request.proposal?.operationID === error.operationID && request.proposal.action === "resume"
                  && canonicalJson(request.proposal.before) === canonicalJson(beforeCore)
                  && canonicalJson(request.proposal.after) === persisted.core_json
                  && canonicalJson(proof.evidence) === canonicalJson(request.proposal)
                  && request.coordinatorEpoch === originalEpoch && recovered?.proof.operation_id === error.operationID
                  && recovered.frame.id === request.frameID && recovered.driver.coordinator_epoch === originalEpoch) {
                core.resolveTransition(error.operationID, "accepted");
                unknownCores.delete(runID);
                if (reopened) store.finishRecovery();
                quarantined.delete(runID); return;
              }
            }
            if (proof && proof.subjectID === runID && (proof.kind === "core.start" || proof.kind === "core.complete" || proof.kind === "core.suspend" || proof.kind === "core.resume") && record && persisted) {
              const request = readTransitionRequest<{ proposal: { operationID: string; action: string; after: Json }; frameID: string; controlCancellation?: boolean; cancellation?: RunView["cancellation"]; completionBarrierID?: string; cleanupRequired?: boolean; input?: Json; inputRef?: Json; decision?: Json; grant?: LeaseGrant; startAssignment?: Json; finalizeAssignment?: Json; wakeAssignment?: Json; validation?: { roundID: string; jobID: string; payload: ValidationPayload; conclusion: { valid: boolean; issue: Json } } | null; roundID?: string; deadline?: number; waitID?: string; waitDeadline?: number | null; initialWake?: Extract<Wake, { kind: "results" }> | null; priorRootActionCount?: number; frameFactIDs?: string[]; actions?: ActionView[]; actionWorkIDs?: string[]; children?: RunView[]; rootAfter?: RunView; remote?: Json; resolved?: PreparedActionRequest[]; revisions?: (number | null)[]; generation?: number; reusedActionWitnesses?: { actionID: string; runID: string; actionKey: string; revision: number; snapshotProjectionID: string; original: { length: number; digest: string }; prepared: { length: number; digest: string } }[]; admissionEvidence?: { id: string; assignmentID: string; assignmentEpoch: number; payload: Json; conclusion: Json }[] }>(store, runID, record.request_json);
              const after = request.proposal.after as { state: string };
              const frame = store.query("SELECT * FROM frames WHERE id=? AND run_id=?", [request.frameID ?? "", runID])[0];
              const view = readRunSnapshot(store, persisted.snapshot_json);
              const candidate = store.query("SELECT decision_json FROM frame_candidates WHERE frame_id=?", [request.frameID ?? ""])[0];
              let validFrame = false;
              if (request.controlCancellation && request.cancellation && canonicalJson(store.effectiveCancellation({ kind: "run", id: runID })) === canonicalJson(request.cancellation)) {
                const work = store.query("SELECT stage,result_json FROM work_items WHERE kind=? AND subject_id=?", ["run", runID]);
                if (proof.kind === "core.start") validFrame = work.length === 1 && work[0]!.stage === "cancel" && view.lifecycle === "started" && view.frameCount === 0 && view.outcome === null;
                if (proof.kind === "core.complete") {
                  const cleanup = store.query("SELECT kind,subject_id,stage FROM work_items WHERE id=?", [JSON.stringify(["cancel.cleanup", runID])])[0];
                  const events = store.query("SELECT data_json FROM history WHERE request_json=? AND type=? AND subject_id=?", [record.request_json as string, "run.completed", runID]);
                  validFrame = work.length === 1 && work[0]!.stage === "done" && view.lifecycle === "completed" && view.outcome?.kind === "cancelled" && view.outcome.reason === request.cancellation.reason && canonicalJson(view.cancellation) === canonicalJson(request.cancellation)
                    && !!cleanup && cleanup.kind === "cancel_cleanup" && cleanup.subject_id === runID && (cleanup.stage === "pending" || cleanup.stage === "done")
                    && events.length === 1 && events[0]!.data_json === canonicalJson({ outcome: view.outcome, completedAt: view.completedAt });
                }
                validFrame = validFrame && work[0]!.result_json === canonicalJson({ proposal: request.proposal, cancellation: request.cancellation });
              }
              if (proof.kind === "core.start" && frame?.state === "open" && sameJsonChunks(request.input, await readFrameInput(store, frame, true)) && view.frameCount === frame.number) {
                const driver = store.query("SELECT * FROM driver_jobs WHERE id=?", [request.frameID])[0];
                const work = store.query("SELECT stage FROM work_items WHERE kind=? AND subject_id=?", ["run", runID]);
                const events = store.query("SELECT data_json FROM history WHERE subject_id=? AND type=?", [runID, "run.frame_created"]);
                const principal = store.query("SELECT principal_id FROM runs WHERE id=?", [runID])[0]?.principal_id;
                validFrame = !!driver && driver.state === "pending" && driver.run_id === runID && driver.agent_id === view.agent.id
                  && driver.agent_version === view.agent.version && driver.principal_id === principal && frameJobInput(driver, frame) !== null
                  && driver.coordinator_epoch === originalEpoch && work.length === 1 && work[0]!.stage === "driver"
                  && events.length === 1 && events[0]!.data_json === canonicalJson({ frameID: request.frameID, frameCount: view.frameCount });
                const start = store.query("SELECT state,assignment_json,grant_json FROM start_jobs WHERE id=?", [runID])[0];
                const lease = store.query("SELECT epoch,node_id,incarnation,released FROM business_leases WHERE kind=? AND subject_id=?", ["run", runID])[0];
                validFrame = validFrame && !!request.grant && !!request.startAssignment && !!start && start.state === "accepted"
                  && start.assignment_json === canonicalJson(request.startAssignment) && start.grant_json === canonicalJson(request.grant)
                  && !!lease && lease.epoch === request.grant.epoch && lease.node_id === request.grant.nodeID
                  && lease.incarnation === request.grant.incarnation && lease.released === 1;
              }
              if (proof.kind === "core.complete" && frame?.state === "committed" && candidate && sameJsonChunks(request.decision, readFrameDecision(store, request.frameID!, candidate.decision_json))) {
                const decision = validateDecision(request.decision);
                const events = store.query("SELECT type,data_json FROM history WHERE subject_id=? AND request_json=? AND type IN ('run.completed','run.decision_committed')", [runID, record.request_json as string]);
                const completion = events.filter(event => event.type === "run.completed");
                const decisionEvents = events.filter(event => event.type === "run.decision_committed");
                const work = store.query("SELECT stage FROM work_items WHERE kind=? AND subject_id=?", ["run", runID]);
                if (decision.kind === "complete" && view.completedAt !== null && sameJsonChunks(view.outcome, decision.outcome)
                    && (request.proposal.after as { outcome?: string }).outcome === decision.outcome.kind
                    && completion.length === 1 && decisionEvents.length === 1 && work.length === 1 && work[0]!.stage === "done"
                    && sameJsonChunks(readRunHistory(store, runID, "run.completed", completion[0]!.data_json), { frameID: request.frameID, outcome: decision.outcome, completedAt: view.completedAt })
                    && sameJsonChunks(readRunHistory(store, runID, "run.decision_committed", decisionEvents[0]!.data_json), { frameID: request.frameID, decision })) {
                  if (decision.outcome.kind !== "succeeded") validFrame = request.validation === null;
                  else if (request.validation) {
                    const validation = request.validation;
                    const round = store.query("SELECT * FROM prepare_rounds WHERE id=?", [validation.roundID])[0];
                    const job = store.query("SELECT * FROM validation_jobs WHERE id=?", [validation.jobID])[0];
                    const principal = store.query("SELECT principal_id FROM runs WHERE id=?", [runID])[0]?.principal_id;
                    const expected = agentOutputValidation(store, view, request.frameID, principal as string, decision, json(validation.payload));
                    validFrame = !!round && !!job && round.owner_kind === "frame" && round.owner_id === request.frameID && round.phase === "output"
                      && round.principal_id === principal && round.state === "accepted" && job.round_id === validation.roundID && job.state === "accepted"
                      && validation.conclusion.valid === true && canonicalJson(validation.payload) === canonicalJson(expected)
                      && job.payload_json === canonicalJson(validation.payload) && sameJsonChunks(readValidationConclusion(store, job.report_json), validation.conclusion);
                  }
                }
                const finalization = store.query("SELECT * FROM finalize_jobs WHERE id=?", [request.frameID])[0];
                const lease = store.query("SELECT epoch,node_id,incarnation,released FROM business_leases WHERE kind=? AND subject_id=?", ["run", runID])[0];
                validFrame = validFrame && !!request.grant && !!request.finalizeAssignment && !!finalization && finalization.state === "accepted"
                  && finalization.assignment_json === canonicalJson(request.finalizeAssignment) && finalization.grant_json === canonicalJson(request.grant)
                  && !!lease && lease.epoch === request.grant.epoch && lease.node_id === request.grant.nodeID
                  && lease.incarnation === request.grant.incarnation && lease.released === 1;
                if (validFrame && finalization?.assignment_encoding === "frame-ref-v1") validFrame = "finalize" in await readFrameAssignment(store, finalization, true);
                const factIDs = store.query("SELECT fact_id FROM frame_facts WHERE frame_id=? AND run_id=? ORDER BY fact_id", [request.frameID, runID]).map(item => item.fact_id);
                validFrame = validFrame && !!request.frameFactIDs && canonicalJson(factIDs) === canonicalJson(request.frameFactIDs)
                  && canonicalJson((await fixedFrameFacts(frame, true)).factIDs.slice().sort()) === canonicalJson(request.frameFactIDs);
                const barrier = store.query("SELECT scope,run_id,cancellation_json FROM cancellation_barriers WHERE id=?", [request.completionBarrierID ?? ""])[0];
                const expectedCancellation = { source: decision.kind === "complete" && decision.outcome.kind === "cancelled" ? "agent" : "completion", reason: decision.kind === "complete" && decision.outcome.kind === "cancelled" ? decision.outcome.reason : null, requestedAt: view.completedAt };
                const cleanup = store.query("SELECT kind,subject_id,stage FROM work_items WHERE id=?", [JSON.stringify(["cancel.cleanup", runID])])[0];
                validFrame = validFrame && !!barrier && barrier.scope === "remaining_actions" && barrier.run_id === runID && barrier.cancellation_json === canonicalJson(expectedCancellation)
                  && typeof request.cleanupRequired === "boolean" && (!request.cleanupRequired || !!cleanup && cleanup.kind === "cancel_cleanup" && cleanup.subject_id === runID && (cleanup.stage === "pending" || cleanup.stage === "done"));
              }
              if (proof.kind === "core.suspend" && frame?.state === "committed" && candidate && sameJsonChunks(request.decision, readFrameDecision(store, request.frameID!, candidate.decision_json)) && request.grant && request.finalizeAssignment && request.roundID && request.waitID && request.actions && request.actionWorkIDs && request.children && request.rootAfter && request.frameFactIDs && request.resolved && request.revisions && request.admissionEvidence && typeof request.generation === "number" && typeof request.deadline === "number" && typeof request.priorRootActionCount === "number") {
                const decision = validateDecision(request.decision);
                const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=? AND frame_id=?", [request.waitID, runID, request.frameID])[0];
                const round = store.query("SELECT state,deadline_at,principal_id FROM prepare_rounds WHERE id=? AND owner_kind=? AND owner_id=?", [request.roundID, "frame", request.frameID])[0];
                const prep = store.query("SELECT * FROM dispatch_preparations WHERE frame_id=? AND round_id=?", [request.frameID, request.roundID])[0];
                const finalization = store.query("SELECT * FROM finalize_jobs WHERE id=?", [request.frameID])[0];
                const lease = store.query("SELECT epoch,node_id,incarnation,released FROM business_leases WHERE kind=? AND subject_id=?", ["run", runID])[0];
                const work = store.query("SELECT stage FROM work_items WHERE kind=? AND subject_id=?", ["run", runID]);
                const facts = store.query("SELECT fact_id FROM frame_facts WHERE frame_id=? AND run_id=? ORDER BY fact_id", [request.frameID, runID]).map(item => item.fact_id);
                const events = store.query("SELECT type,data_json FROM history WHERE subject_id=? AND request_json=? AND type IN ('run.decision_committed','run.wait_created')", [runID, record.request_json as string]);
                const decisionEvents = events.filter(item => item.type === "run.decision_committed" && sameJsonChunks(readRunHistory(store, runID, "run.decision_committed", item.data_json), { frameID: request.frameID, decision }));
                const waitEvents = events.filter(item => item.type === "run.wait_created" && item.data_json === canonicalJson({ waitID: request.waitID, frameID: request.frameID, deadlineAt: request.waitDeadline }));
                const waitCreated = store.query("SELECT data_json FROM history WHERE subject_id=? AND type=?", [runID, "wait.created"])
                  .filter(item => item.data_json === canonicalJson({ waitID: request.waitID, frameID: request.frameID, actionKeys: decision.kind === "dispatch" ? decision.wait.all : [], deadlineAt: request.waitDeadline }));
                validFrame = decision.kind === "dispatch" && view.lifecycle === "suspended" && view.rootActionCount === request.priorRootActionCount + request.actions.length
                  && view.wait?.id === request.waitID && view.wait.deadlineAt === request.waitDeadline && view.wait.winner === (request.initialWake ? "results" : null) && canonicalJson(view.wait.actionKeys) === canonicalJson(decision.wait.all)
                  && sameJsonChunks(view.continuation, decision.continuation) && !!wait && wait.action_keys_json === canonicalJson(decision.wait.all)
                  && wait.deadline_at === request.waitDeadline && wait.winner === (request.initialWake ? "results" : null)
                  && (request.initialWake ? sameJsonChunks(await readWaitWake(store, wait, runID, true), request.initialWake) : wait.wake_json === null) && round?.state === "accepted" && round.deadline_at === request.deadline
                  && !!prep && prep.state === "ready" && prep.generation === request.generation && sameJsonChunks(readPreparationResolved(store, prep), request.resolved)
                  && prep.pointer_revisions_json === canonicalJson(request.revisions) && request.resolved.length === request.actions.length
                  && request.revisions.length === request.actions.length && request.admissionEvidence.length === request.actions.length
                  && finalization?.state === "accepted" && finalization.assignment_json === canonicalJson(request.finalizeAssignment) && finalization.grant_json === canonicalJson(request.grant)
                  && lease?.epoch === request.grant.epoch && lease.node_id === request.grant.nodeID && lease.incarnation === request.grant.incarnation && lease.released === 1
                  && work.length === 1 && work[0]!.stage === (request.initialWake ? "wake" : "waiting") && canonicalJson(facts) === canonicalJson(request.frameFactIDs)
                  && canonicalJson((await fixedFrameFacts(frame, true)).factIDs.slice().sort()) === canonicalJson(request.frameFactIDs)
                  && decisionEvents.length === 1 && waitEvents.length === 1 && waitCreated.length === 1 && request.actions.length === request.actionWorkIDs.length;
                if (validFrame && finalization?.assignment_encoding === "frame-ref-v1") validFrame = "finalize" in await readFrameAssignment(store, finalization, true);
                const persistedRoot = store.query("SELECT principal_id,snapshot_json FROM runs WHERE id=?", [view.rootID])[0];
                validFrame = validFrame && !!persistedRoot && persistedRoot.principal_id === round?.principal_id && sameJsonChunks(readRunSnapshot(store, persistedRoot.snapshot_json), request.rootAfter)
                  && request.rootAfter.id === view.rootID && request.rootAfter.rootActionCount === request.priorRootActionCount + request.actions.length
                  && request.children.length === request.actions.filter(action => action.request.kind === "agent").length;
                if (validFrame && request.initialWake) {
                  validFrame = hasWaitResolutionEvents(store, runID, request.waitID, request.initialWake.facts);
                }
                if (validFrame) for (const [index, action] of request.actions.entries()) {
                  const original = readPreparationOriginals(store, prep!)[index];
                  const policies = readPreparationPolicies(store, prep!);
                  const evidence = request.admissionEvidence[index]!;
                  const validationJob = store.query("SELECT state,assignment_id,assignment_epoch,payload_json,report_json,round_id FROM validation_jobs WHERE id=?", [evidence.id])[0];
                  const expectedPayload = { kind: action.request.kind, ref: action.request.target, phase: "admission", value: action.request.input, config: action.request.config, principalID: round!.principal_id,
                    operation: "client.submit", context: { domainID: store.domainID, rootID: view.rootID, runID, frameID: request.frameID, decision, policies, original, prepared: action.request, pointerRevision: request.revisions[index] } };
                  const actionRow = store.query("SELECT id,run_id,action_key,revision,request_json,snapshot_json,snapshot_projection_id FROM actions WHERE id=?", [action.id])[0];
                  const actionWork = store.query("SELECT kind,subject_id,stage FROM work_items WHERE id=?", [request.actionWorkIDs[index]!])[0];
                  const acceptedEvents = store.query("SELECT data_json FROM history WHERE subject_id=? AND type=?", [action.id, "action.accepted"]);
                  // A fresh Action in the original suspend receipt is the
                  // initial inline revision. A later projected terminal
                  // snapshot is not evidence that this original commit won.
                  if (!actionRow || actionRow.id !== action.id || actionRow.run_id !== runID || actionRow.action_key !== action.request.key || actionRow.revision !== action.revision
                      || actionRow.snapshot_projection_id !== null || !fixedActionRequestMatches(store, action.id, action.request, actionRow.request_json)
                      || !sameJsonChunks(expandActionRequests(store, decode<Json>(actionRow.snapshot_json)), action) || !actionWork
                      || (action.request.kind === "capacity" ? actionWork.kind !== "action" || actionWork.subject_id !== action.id || actionWork.stage !== "accepted"
                        : actionWork.kind !== "run" || actionWork.subject_id !== action.remoteRunID || actionWork.stage !== "start"
                          || !request.children!.some(child => child.id === action.remoteRunID && child.parentActionID === action.id && child.rootID === view.rootID
                            && canonicalJson(child.agent) === canonicalJson(action.request.target) && canonicalJson(child.input) === canonicalJson(action.request.input)
                            && canonicalJson(child.config) === canonicalJson(action.request.config) && canonicalJson(child.limits) === canonicalJson(view.limits)
                            && !!store.query("SELECT id FROM runs WHERE id=? AND parent_action_id=? AND principal_id=? AND snapshot_json=?", [child.id, action.id, round!.principal_id as string, expectedRunSnapshot(store, child)])[0]
                            && store.query("SELECT data_json FROM history WHERE type='run.accepted' AND subject_id=? AND request_json=?", [child.id, record.request_json as string]).filter(event =>
                              sameJsonChunks(readRunHistory(store, child.id, "run.accepted", event.data_json), { runID: child.id, rootID: child.rootID, parentActionID: child.parentActionID, principalID: round!.principal_id, agent: child.agent, input: child.input, config: child.config, limits: child.limits })).length === 1
                            && !!store.query("SELECT action_id FROM remote_run_links WHERE action_id=? AND child_run_id=?", [action.id, child.id])[0]))
                      || canonicalJson(action.request) !== canonicalJson(request.resolved[index]) || canonicalJson(action.original) !== canonicalJson(original)
                      || !dispatchPayloadMatches(expectedPayload, evidence.payload) || (evidence.conclusion as { valid?: boolean }).valid !== true
                      || !validationJob || validationJob.state !== "accepted" || validationJob.round_id !== request.roundID || validationJob.assignment_id !== evidence.assignmentID
                      || validationJob.assignment_epoch !== evidence.assignmentEpoch || validationJob.payload_json !== canonicalJson(evidence.payload) || !sameJsonChunks(readValidationConclusion(store, validationJob.report_json), evidence.conclusion)
                      || acceptedEvents.length !== 1 || !sameJsonChunks(readActionAcceptance(store, action.id, acceptedEvents[0]!.data_json), { runID, frameID: request.frameID, actionID: action.id, original: action.original, request: action.request })) { validFrame = false; break; }
                }
                if (validFrame && decision.kind === "dispatch") {
                  const witnesses = request.reusedActionWitnesses ?? [], seen = new Set<string>();
                  if (!Array.isArray(witnesses)) validFrame = false;
                  else for (const witness of witnesses) {
                    if (!witness || typeof witness.actionKey !== "string" || seen.has(witness.actionKey) || witness.runID !== runID
                        || !decision.wait.all.includes(witness.actionKey) || request.actions.some(action => action.request.key === witness.actionKey)) { validFrame = false; break; }
                    seen.add(witness.actionKey);
                    const previous = store.query("SELECT * FROM actions WHERE id=? AND run_id=? AND action_key=?", [witness.actionID, runID, witness.actionKey])[0];
                    if (!previous || previous.revision !== witness.revision || previous.snapshot_projection_id !== witness.snapshotProjectionID) { validFrame = false; break; }
                    const identity = await readActionReuseIdentity(store, previous, true);
                    const decided = decision.actions.find(action => action.key === witness.actionKey);
                    if (decided && !sameJsonChunks(identity.original, decided) || !sameJsonChunks(await canonicalValueWitness(identity.original), witness.original)
                        || !sameJsonChunks(await canonicalValueWitness(identity.request), witness.prepared)) { validFrame = false; break; }
                  }
                  if (validFrame) for (const key of decision.wait.all) {
                    if (request.actions.some(action => action.request.key === key)) continue;
                    const previous = store.query("SELECT snapshot_projection_id FROM actions WHERE run_id=? AND action_key=?", [runID, key])[0];
                    if (!previous || previous.snapshot_projection_id !== null && !seen.has(key)) { validFrame = false; break; }
                  }
                }
              }
              if (proof.kind === "core.resume" && frame?.state === "open" && (request.input || request.inputRef) && request.waitID && request.grant && request.wakeAssignment) {
                const input = await readFrameInput(store, frame, true);
                const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=?", [request.waitID, runID])[0];
                const previousFrame = wait && store.query("SELECT number FROM frames WHERE id=? AND run_id=?", [wait.frame_id as string, runID])[0];
                const wakeJob = store.query("SELECT state,assignment_json,grant_json FROM wake_jobs WHERE id=? AND run_id=?", [request.waitID, runID])[0];
                const lease = store.query("SELECT epoch,node_id,incarnation,released FROM business_leases WHERE kind=? AND subject_id=?", ["run", runID])[0];
                const driver = store.query("SELECT * FROM driver_jobs WHERE id=?", [request.frameID])[0];
                const work = store.query("SELECT stage FROM work_items WHERE kind=? AND subject_id=?", ["run", runID]);
                const linkedFacts = store.query("SELECT fact_id FROM frame_facts WHERE frame_id=? AND run_id=? ORDER BY fact_id", [request.frameID, runID]).map(item => item.fact_id);
                const expectedFacts = input.wake.kind === "start" ? [] : input.wake.facts.map(fact => fact.id).sort();
                const events = store.query("SELECT data_json FROM history WHERE subject_id=? AND type=?", [runID, "run.frame_created"])
                  .filter(item => item.data_json === canonicalJson({ frameID: request.frameID, frameCount: view.frameCount }));
                const expectedRun = { id: view.id, rootID: view.rootID, parentActionID: view.parentActionID, agent: view.agent, input: view.input, config: view.config, limits: view.limits };
                const owner = store.query("SELECT principal_id FROM runs WHERE id=?", [runID])[0];
                validFrame = (frame.input_encoding === "inline" ? !!request.input && sameJsonChunks(request.input, input)
                  : !!request.inputRef && canonicalJson(request.inputRef) === frame.input_json) && input.frameID === request.frameID && canonicalJson(input.run) === canonicalJson(expectedRun)
                  && input.wake.kind !== "start" && input.wake.waitID === request.waitID && !!wait && wait.resumed_frame_id === request.frameID
                  && wait.winner === input.wake.kind && sameJsonChunks(await readWaitWake(store, wait, runID, true), input.wake) && view.lifecycle === "resumed" && view.wait === null
                  && !!previousFrame && frame.number === (previousFrame.number as number) + 1 && view.frameCount === frame.number && sameJsonChunks(view.continuation, input.continuation)
                  && canonicalJson(linkedFacts) === canonicalJson(expectedFacts) && wakeJob?.state === "accepted"
                  && wakeJob.assignment_json === canonicalJson(request.wakeAssignment) && wakeJob.grant_json === canonicalJson(request.grant)
                  && lease?.epoch === request.grant.epoch && lease.node_id === request.grant.nodeID && lease.incarnation === request.grant.incarnation && lease.released === 1
                  && !!driver && driver.state === "pending" && driver.run_id === runID && driver.agent_id === view.agent.id && driver.agent_version === view.agent.version
                  && frameJobInput(driver, frame) !== null && driver.principal_id === owner?.principal_id && driver.coordinator_epoch === originalEpoch && work.length === 1 && work[0]!.stage === "driver" && events.length === 1;
                if (validFrame && input.wake.kind !== "start") for (const fact of input.wake.facts) {
                  const actual = store.query("SELECT f.action_id,f.outcome_json,f.outcome_projection_id,a.action_key FROM facts f JOIN actions a ON a.id=f.action_id WHERE f.id=? AND f.run_id=?", [fact.id, runID])[0];
                  if (!actual || actual.action_id !== fact.actionID || actual.action_key !== fact.actionKey || !sameJsonChunks(await readFactOutcomeForEvidence(store, actual), fact.outcome)) { validFrame = false; break; }
                }
              }
              if (proof.kind === "core.complete") validFrame = validFrame && await verifyRemoteCompletionReferenced(store, view, error.operationID, decode<Json>(record.request_json));
              if (request.proposal.operationID === error.operationID && request.proposal.action === proof.kind.slice(5)
                  && canonicalJson(proof.evidence) === canonicalJson(request.proposal) && canonicalJson(request.proposal.after) === persisted.core_json
                  && after.state === view.lifecycle && validFrame) {
                core.resolveTransition(error.operationID, "accepted");
                unknownCores.delete(runID);
                if (reopened) { store.finishRecovery(); quarantined.delete(runID); return; }
                if (request.controlCancellation || proof.kind === "core.suspend" || proof.kind === "core.resume") { quarantined.delete(runID); return; }
              }
            }
            const witnessHeld = witness !== undefined;
            const unchanged = witnessHeld && (store.recoveryWitnessUnchanged(witness) || recoveryScopeMatches(runID))
              && persisted && persisted.core_json === (beforeCore === null ? null : canonicalJson(beforeCore));
            if (reopened && !proof && unchanged && (previous === null || transitionBefore(runID) === previous)) {
              core.resolveTransition(error.operationID, "rejected");
              unknownCores.delete(runID);
              store.finishRecovery(); quarantined.delete(runID); rejected = true;
            }
          }
        } catch { /* keep isolated until a recovery bridge can establish proof */ }
        if (rejected) throw new CoreError("CONFIRMATION_REJECTED", "Original Core operation was not committed after closing the old transaction.");
      }
      throw error;
    } finally { transitionsInFlight.delete(runID); }
  }
  function block(work: Row, view: RunView, code: string, message: string, details: Json, extra: SQLWrite[] = [], validBefore?: number, absent?: AbsenceGuard[]) {
    const updated: RunView = { ...view, revision: view.revision + 1, updatedAt: now(), blocked: { code, message, details } };
    commit(randomUUID(), "run.blocked", view.id, { code, details }, [runWrite(updated, view), stage(work.id as string, work.stage as string, "blocked"), ...extra], null,
      [{ type: "run.blocked", subjectID: view.id, data: json({ blocked: updated.blocked }) }], validBefore, absent);
  }
  async function resolveWait(work: Row, view: RunControl): Promise<void> {
    if (!view.wait || view.wait.winner !== null || view.lifecycle !== "suspended") return;
    // Poll only control and fact identities. Large state/fact values are read
    // once a winner exists, rather than reconstructed on every waiting scan.
    const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=?", [view.wait.id, view.id])[0];
    if (!wait || wait.winner !== null || wait.action_keys_json !== canonicalJson(view.wait.actionKeys) || wait.deadline_at !== view.wait.deadlineAt) return;
    const facts: Fact[] = [], pendingActionKeys: string[] = [], factSnapshot: { actionID: string; factID: string | null }[] = [];
    const factRows: { row: Row; key: string; actionID: string }[] = [];
    for (const key of view.wait.actionKeys) {
      const action = store.query("SELECT id FROM actions WHERE run_id=? AND action_key=?", [view.id, key])[0];
      if (!action) return;
      const fact = store.query("SELECT id,action_id,outcome_json,outcome_projection_id FROM facts WHERE action_id=? AND run_id=?", [action.id as string, view.id])[0];
      factSnapshot.push({ actionID: action.id as string, factID: fact ? fact.id as string : null });
      if (fact) factRows.push({ row: fact, key, actionID: action.id as string });
      else pendingActionKeys.push(key);
    }
    const at = now(), winner = pendingActionKeys.length === 0 ? "results" : view.wait.deadlineAt !== null && at >= view.wait.deadlineAt ? "timeout" : null;
    if (!winner) return;
    const current = store.query("SELECT snapshot_json FROM runs WHERE id=? AND revision=?", [view.id, view.revision])[0];
    if (!current) return;
    const native = readRunSnapshot(store, current.snapshot_json);
    if (!native.wait || native.wait.id !== view.wait.id || native.wait.winner !== null || native.outcome || native.cancelRequested) return;
    for (const { row, key, actionID } of factRows) facts.push({ id: row.id as string, actionID, actionKey: key, outcome: await readFactOutcome(store, row) });
    const wake: Wake = winner === "results" ? { kind: "results", waitID: view.wait.id, facts } : { kind: "timeout", waitID: view.wait.id, facts, pendingActionKeys };
    const updated: RunView = { ...native, wait: { ...native.wait, winner }, revision: native.revision + 1, updatedAt: at };
    const witnesses: Json[] = [];
    for (const fact of facts) witnesses.push(await waitFactWitness(fact));
    const history = referencedWaitResolutionEvents(store, view.id, view.wait.id, facts, options.maxResponseBytes, winner === "timeout" ? pendingActionKeys : undefined);
    const projected = history.events.some(event => event.dataProjectionID !== undefined);
    const request = json({ encoding: projected ? "wait-fact-witness-v2" : "wait-fact-witness-v1", waitID: view.wait.id, winner, facts: witnesses, pendingActionKeys,
      ...(projected ? { eventProjectionIDs: history.events.map(event => event.dataProjectionID ?? null) } : {}) });
    store.commit({ operationID: `wait.resolve:${view.wait.id}:${winner}`, kind: "wait.resolve", subjectID: view.id, request,
      factSnapshot, writes: [cancellationGuard({ kind: "run", id: view.id }), ...history.writes, write("UPDATE waits SET winner=?,wake_json=? WHERE id=? AND run_id=? AND winner IS NULL AND action_keys_json=? AND deadline_at IS ?", [winner, { json: request }, view.wait.id, view.id, wait.action_keys_json as string, wait.deadline_at as number | null]),
        runWrite(updated, native), write("UPDATE work_items SET stage=?,result_json=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["wake", null, work.id as string, "run", view.id, "waiting"])],
      events: history.events, evidence: null, result: null });
  }
  function commitControlStep(batch: CommitBatch, row: Row, afterWork: Row, core: RunCore | undefined, released: boolean): void {
    const beforeWork = store.query("SELECT * FROM work_items WHERE id=?", [afterWork.id as string])[0];
    if (!beforeWork) throw new Error("Missing original cancellation control work");
    const sealed = { batch: json(batch) as unknown as CommitBatch, epoch: store.coordinatorEpoch,
      run: canonicalJson(row), beforeWork: canonicalJson(beforeWork), work: canonicalJson(afterWork), core, snapshot: canonicalJson(core?.getSnapshot() ?? null), released };
    try { store.commit(batch); }
    catch (error) {
      if (isUncertain(error) && store.requiresRecovery && store.unresolvedCommit?.operationID === batch.operationID) sealedControl = sealed;
      throw error;
    }
  }
  async function cancelRun(work: Row, row: Row, view: RunView) {
    const runID = view.id, cancellation = store.effectiveCancellation({ kind: "run", id: runID });
    if (!cancellation || view.outcome || quarantined.has(runID) || transitionsInFlight.has(runID)) return;
    if (!cores.has(runID) && (view.lifecycle === "started" || view.lifecycle === "resumed")) {
      // This restricted bridge only enables cancellation. Reassigning a live
      // business frame also requires recovery of the immutable job epochs.
      if (!(coldControlStarts.has(runID) ? recoverControlStart(work, row, view, cancellation) : await recoverActive(work, row, view))) quarantined.add(runID);
      return;
    }
    // Arm the control before Core is invoked. A cold coordinator cannot reconstruct an
    // in-flight Core handle, even if no proposal was durably recorded yet.
    commitControlStep({ operationID: randomUUID(), kind: "cancel.control_arm", subjectID: runID, request: json({ runID, cancellation }),
      writes: [write("UPDATE work_items SET stage=?,result_json=? WHERE id=? AND stage=?", ["cancel_control", null, work.id as string, "cancel"])], evidence: null, result: null },
      row, { ...work, stage: "cancel_control", result_json: null }, cores.get(runID), true);
    let core = cores.get(runID);
    if (!core) {
      if (!row.core_json && view.lifecycle === null) core = createRunCore(runID);
      else if (row.core_json && view.lifecycle === "suspended") {
        try { core = restoreRunCore(decode(row.core_json)); if (core.getSnapshot()?.state !== "suspended" || core.getSnapshot()?.runID !== runID) throw new Error("Invalid suspended snapshot"); }
        catch { quarantined.add(runID); return; }
      } else { quarantined.add(runID); return; }
      cores.set(runID, core);
    }
    const active = core;
    const recordIntent = (proposal: unknown) => {
      const currentWork = store.query("SELECT * FROM work_items WHERE id=?", [work.id as string])[0];
      if (!currentWork) throw new Error("Missing cancellation control work");
      commitControlStep({ operationID: randomUUID(), kind: "cancel.control_intent", subjectID: runID,
        request: json({ proposal, cancellation }), writes: [write("UPDATE work_items SET result_json=? WHERE id=? AND stage=?", [{ json: json({ proposal, cancellation }) }, work.id as string, "cancel_control"])], evidence: json(proposal), result: null },
        row, { ...currentWork, result_json: canonicalJson({ proposal, cancellation }) }, active, false);
    };
    let intentUncertain = false;
    const controlTransition = async (invoke: () => Promise<unknown>) => {
      try { await transition(runID, active, invoke); }
      catch (error) {
        if (error instanceof CoreError && error.code === "CONFIRMATION_REJECTED" && intentUncertain && sealedControl?.core === active) sealedControl.released = true;
        if (error instanceof CoreError && error.code === "CONFIRMATION_REJECTED" && !intentUncertain) {
          store.commit({ operationID: randomUUID(), kind: "cancel.control_retry", subjectID: runID, request: json({ runID }), writes: [stage(work.id as string, "cancel_control", "cancel")], evidence: null, result: null });
        }
        throw error;
      }
    };
    if (!active.getSnapshot()) {
      const at = now();
      await controlTransition(() => active.start(async proposal => {
        const updated: RunView = { ...view, lifecycle: "started", revision: view.revision + 1, updatedAt: at };
        try { recordIntent(proposal); }
        catch (error) { intentUncertain = isUncertain(error); return "rejected"; }
        try {
          noteRecoveryScope(runID);
          store.commit({ operationID: proposal.operationID, kind: "core.start", subjectID: runID, request: json({ proposal, controlCancellation: true, cancellation }),
            writes: [runWrite(updated, view, json(proposal.after), null), stage(work.id as string, "cancel_control", "cancel")], evidence: json(proposal), result: null });
          return "accepted";
        } catch (error) { return isUncertain(error) ? "unknown" : "rejected"; }
      }));
      return;
    }
    const snapshot = active.getSnapshot()!;
    if (canonicalJson(snapshot) !== row.core_json || !["started", "resumed", "suspended"].includes(snapshot.state)) return;
    const at = now();
    await controlTransition(() => active.complete({ executionToken: snapshot.executionToken, outcome: "cancelled" }, async proposal => {
      const updated: RunView = { ...view, lifecycle: "completed", cancelRequested: true, cancellation, blocked: null,
        outcome: { kind: "cancelled", reason: cancellation.reason }, completedAt: at, updatedAt: at, revision: view.revision + 1 };
      try { recordIntent(proposal); }
      catch (error) { intentUncertain = isUncertain(error); return "rejected"; }
      try {
        const remote = await prepareRemoteCompletion(store, view, updated, proposal.operationID, at, options.maxResponseBytes);
        noteRecoveryScope(runID);
        store.commit({ operationID: proposal.operationID, kind: "core.complete", subjectID: runID, request: json({ proposal, controlCancellation: true, cancellation, remote: remote?.evidence ?? null }),
          writes: [runWrite(updated, view, json(proposal.after), json(proposal.before)), stage(work.id as string, "cancel_control", "done"), ...(remote?.writes ?? [])], evidence: json(proposal), result: null,
          events: [{ type: "run.completed", subjectID: runID, data: json({ outcome: updated.outcome, completedAt: at }) }, ...(remote?.events ?? [])], ...(remote ? { factSnapshot: remote.factSnapshot } : {}) });
        return "accepted";
      } catch (error) { return isUncertain(error) ? "unknown" : "rejected"; }
    }));
    if (!quarantined.has(runID)) cores.delete(runID);
  }
  function recoverControlStart(work: Row, row: Row, view: RunView, cancellation: NonNullable<RunView["cancellation"]>): boolean {
    try {
      const active = decode<StartedSnapshot>(row.core_json);
      if (!active || canonicalJson(Object.keys(active).sort()) !== canonicalJson(["executionToken", "outcome", "runID", "state", "suspendRequested"])
          || active.state !== "started" || active.runID !== view.id || typeof active.executionToken !== "string" || !active.executionToken.trim()
          || canonicalJson(startSnapshot(active.runID, null, active.executionToken)) !== row.core_json || view.lifecycle !== "started" || view.frameCount !== 0
          || store.query("SELECT id FROM frames WHERE run_id=? LIMIT 1", [view.id]).length) return false;
      const proof = store.query("SELECT * FROM history WHERE subject_id=? AND type IN ('core.start','core.resume','core.suspend','core.complete','core.recovery_suspend','core.recovery_resume') ORDER BY sequence DESC LIMIT 1", [view.id])[0];
      if (!proof || proof.type !== "core.start" || typeof proof.operation_id !== "string") return false;
      const request = decode<{ proposal: { operationID: string; action: string; before: Json; after: Json }; controlCancellation: boolean; cancellation: RunView["cancellation"] }>(proof.request_json);
      if (!request.proposal || request.controlCancellation !== true || request.proposal.operationID !== proof.operation_id || request.proposal.action !== "start"
          || request.proposal.before !== null || canonicalJson(request.proposal.after) !== row.core_json || canonicalJson(request.proposal) !== proof.data_json
          || canonicalJson(request.cancellation) !== canonicalJson(cancellation)) return false;
      const intentRequest = canonicalJson({ proposal: request.proposal, cancellation });
      const intent = store.query("SELECT data_json FROM history WHERE type='cancel.control_intent' AND subject_id=? AND request_json=?", [view.id, intentRequest]);
      if (intent.length !== 1 || intent[0]!.data_json !== canonicalJson(request.proposal)) return false;
      const after = suspendSnapshot(active, active.executionToken), operationID = randomUUID();
      const proposal = { operationID, action: "suspend", before: active, after };
      const updated: RunView = { ...view, lifecycle: "suspended", revision: view.revision + 1, updatedAt: now() };
      store.commit({ operationID, kind: "core.recovery_suspend", subjectID: view.id, request: json({ proposal, sourceOperationID: proof.operation_id, controlCancellation: true }),
        writes: [runWrite(updated, view, json(after), json(active)), stage(work.id as string, "cancel", "cancel")], evidence: json(proposal), result: null });
      coldControlStarts.delete(view.id);
      return true;
    } catch (error) {
      return error instanceof HarnessError && error.info.code === "REVISION_CONFLICT" && error.info.disposition !== "uncertain";
    }
  }
  function recoverColdControlIntent(work: Row): boolean {
    try {
      if (coldControlIntents.get(work.id as string) !== work.result_json) return false;
      const pending = decode<{ proposal: { operationID: string; action: string; before: Json; after: Json }; cancellation: NonNullable<RunView["cancellation"]> }>(work.result_json);
      const proposal = pending.proposal;
      if (!proposal || typeof proposal.operationID !== "string" || !proposal.operationID.trim()
          || canonicalJson(Object.keys(proposal).sort()) !== canonicalJson(["action", "after", "before", "operationID"])
          || canonicalJson(store.effectiveCancellation({ kind: "run", id: work.subject_id as string })) !== canonicalJson(pending.cancellation)) return false;
      const intent = store.query("SELECT data_json FROM history WHERE type='cancel.control_intent' AND subject_id=? AND request_json=?", [work.subject_id as string, canonicalJson(pending)]);
      if (intent.length !== 1 || intent[0]!.data_json !== canonicalJson(proposal) || store.getCommit(proposal.operationID)) return false;
      const row = store.query("SELECT * FROM runs WHERE id=?", [work.subject_id as string])[0];
      if (!row || row.core_json !== (proposal.before === null ? null : canonicalJson(proposal.before))) return false;
      if (proposal.action === "start") {
        const after = proposal.after as unknown as StartedSnapshot;
        if (proposal.before !== null || !after || typeof after.executionToken !== "string" || !after.executionToken.trim()
            || after.runID !== work.subject_id || canonicalJson(startSnapshot(after.runID, null, after.executionToken)) !== canonicalJson(after)) return false;
      } else if (proposal.action === "complete") {
        const before = proposal.before as unknown as StartedSnapshot | ResumedSnapshot | { state: "suspended"; runID: StartedSnapshot["runID"]; executionToken: null; suspendRequested: false; outcome: null };
        if (!before || before.runID !== work.subject_id || !["started", "resumed", "suspended"].includes(before.state)
            || before.state === "suspended" && canonicalJson(restoreRunCore(before).getSnapshot()) !== canonicalJson(before)
            || canonicalJson(completeSnapshot(before, before.executionToken, "cancelled")) !== canonicalJson(proposal.after)) return false;
      } else return false;
      store.commit({ operationID: randomUUID(), kind: "cancel.control_recovered_uncommitted", subjectID: work.subject_id as string,
        request: json({ originalOperationID: proposal.operationID, proposal, cancellation: pending.cancellation }),
        writes: [write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND core_json IS ?", [work.subject_id as string, row.revision as number, row.snapshot_json as string, row.core_json as string | null]),
          write("UPDATE work_items SET stage=?,result_json=NULL WHERE id=? AND stage=? AND result_json=?", ["cancel", work.id as string, "cancel_control", work.result_json as string])],
        evidence: json({ originalOperationID: proposal.operationID, before: proposal.before }), result: null });
      coldControlIntents.delete(work.id as string);
      return true;
    } catch (error) {
      return error instanceof HarnessError && error.info.code === "REVISION_CONFLICT" && error.info.disposition !== "uncertain";
    }
  }
  async function activeFrameEvidence(row: Row, view: RunView, activeJSON: string, beforeSequence?: number, depth = 0, recoveryEvidence = false): Promise<{ active: StartedSnapshot | ResumedSnapshot; proof: Row; frame: Row; driver: Row } | null> {
      const evidenceEpoch = store.coordinatorEpoch;
      const stillAuthoritative = () => { store.assertEvidenceReadAuthority(); return store.coordinatorEpoch === evidenceEpoch; };
      if (depth > 1024) return null;
      const value = decode<Record<string, unknown>>(activeJSON);
      if (!value || typeof value !== "object" || Array.isArray(value)
          || canonicalJson(Object.keys(value).sort()) !== canonicalJson(["executionToken", "outcome", "runID", "state", "suspendRequested"])
          || value.runID !== view.id || value.state !== view.lifecycle || !["started", "resumed"].includes(value.state as string)
          || typeof value.executionToken !== "string" || !value.executionToken.trim() || typeof value.suspendRequested !== "boolean" || value.outcome !== null) return null;
      const active = value as unknown as StartedSnapshot | ResumedSnapshot;
      const proof = store.query(`SELECT * FROM history WHERE subject_id=? AND type IN ('core.start','core.resume','core.suspend','core.complete','core.recovery_suspend','core.recovery_resume')${beforeSequence === undefined ? "" : " AND sequence<?"} ORDER BY sequence DESC LIMIT 1`, [view.id, ...(beforeSequence === undefined ? [] : [beforeSequence])])[0];
      if (!proof || typeof proof.operation_id !== "string") return null;
      if (proof.type === "core.recovery_resume") {
        if (active.state !== "resumed" || typeof proof.sequence !== "number") return null;
        const resumed = decode<{ proposal: { operationID: string; action: string; before: Json; after: Json }; frameID: string; sourceBridgeID: string; oldCoordinatorEpoch: number; coordinatorEpoch: number }>(proof.request_json);
        if (!resumed.proposal || resumed.proposal.operationID !== proof.operation_id || resumed.proposal.action !== "resume"
            || canonicalJson(resumed.proposal) !== proof.data_json || canonicalJson(resumed.proposal.after) !== activeJSON
            || !Number.isSafeInteger(resumed.oldCoordinatorEpoch) || !Number.isSafeInteger(resumed.coordinatorEpoch) || resumed.coordinatorEpoch <= resumed.oldCoordinatorEpoch) return null;
        const prior = restoreRunCore(resumed.proposal.before).getSnapshot();
        if (!prior || prior.state !== "suspended" || canonicalJson(resumeSnapshot(prior, active.executionToken)) !== activeJSON) return null;
        const bridge = store.query("SELECT * FROM history WHERE subject_id=? AND type IN ('core.start','core.resume','core.suspend','core.complete','core.recovery_suspend','core.recovery_resume') AND sequence<? ORDER BY sequence DESC LIMIT 1", [view.id, proof.sequence])[0];
        if (!bridge || bridge.type !== "core.recovery_suspend" || bridge.operation_id !== resumed.sourceBridgeID || typeof bridge.sequence !== "number") return null;
        const suspended = decode<{ proposal: { operationID: string; action: string; before: Json; after: Json }; frameID: string; sourceOperationID: string; oldCoordinatorEpoch: number }>(bridge.request_json);
        if (!suspended.proposal || suspended.proposal.operationID !== bridge.operation_id || suspended.proposal.action !== "suspend"
            || canonicalJson(suspended.proposal) !== bridge.data_json || canonicalJson(suspended.proposal.after) !== canonicalJson(prior)
            || suspended.frameID !== resumed.frameID || suspended.oldCoordinatorEpoch !== resumed.oldCoordinatorEpoch) return null;
        const previous = suspended.proposal.before as { state?: RunView["lifecycle"] };
        const source = await activeFrameEvidence(row, { ...view, lifecycle: previous?.state ?? null }, canonicalJson(suspended.proposal.before), bridge.sequence, depth + 1, recoveryEvidence);
        if (!source || source.proof.operation_id !== suspended.sourceOperationID || source.frame.id !== resumed.frameID
            || resumed.coordinatorEpoch > Number(source.driver.coordinator_epoch)
            || canonicalJson(suspendSnapshot(source.active, source.active.executionToken)) !== canonicalJson(prior)) return null;
        return stillAuthoritative() ? { active, proof, frame: source.frame, driver: source.driver } : null;
      }
      if (proof.type !== `core.${active.state === "started" ? "start" : "resume"}`) return null;
      const request = readTransitionRequest<{ proposal: { operationID: string; action: string; before: Json; after: Json }; frameID: string; input?: AdvanceInput; inputRef?: Json; waitID?: string }>(store, view.id, proof.request_json);
      if (!request.proposal || request.proposal.operationID !== proof.operation_id || request.proposal.action !== (active.state === "started" ? "start" : "resume")
          || canonicalJson(Object.keys(request.proposal).sort()) !== canonicalJson(["action", "after", "before", "operationID"])
          || canonicalJson(request.proposal) !== proof.data_json || canonicalJson(request.proposal.after) !== activeJSON || !request.frameID || !request.input && !request.inputRef) return null;
      const frame = store.query("SELECT * FROM frames WHERE id=? AND run_id=? AND state='open'", [request.frameID, view.id])[0];
      const driver = store.query("SELECT * FROM driver_jobs WHERE id=? AND run_id=?", [request.frameID, view.id])[0];
      const expectedRun = { id: view.id, rootID: view.rootID, parentActionID: view.parentActionID, agent: view.agent, input: view.input, config: view.config, limits: view.limits };
      if (!frame) return null;
      const input = await readFrameInput(store, frame, recoveryEvidence);
      if (frame.number !== view.frameCount || (frame.input_encoding === "inline" ? !request.input || !sameJsonChunks(request.input, input)
            : !request.inputRef || canonicalJson(request.inputRef) !== frame.input_json)
          || input.frameID !== frame.id || !sameJsonChunks(input.run, expectedRun)
          || !sameJsonChunks(input.continuation, view.continuation)
          || !driver || driver.agent_id !== view.agent.id || driver.agent_version !== view.agent.version || driver.principal_id !== row.principal_id
          || frameJobInput(driver, frame) === null || typeof driver.coordinator_epoch !== "number" || !Number.isSafeInteger(driver.coordinator_epoch) || driver.coordinator_epoch < 1 || driver.coordinator_epoch >= store.coordinatorEpoch
          || store.query("SELECT id FROM history WHERE type='run.frame_created' AND subject_id=? AND request_json=? AND data_json=?", [view.id, proof.request_json as string, canonicalJson({ frameID: frame.id, frameCount: view.frameCount })]).length !== 1) return null;
      if (active.state === "started") {
        if (request.proposal.before !== null || input.wake.kind !== "start" || view.frameCount !== 1
            || canonicalJson(startSnapshot(active.runID, null, active.executionToken)) !== activeJSON) return null;
      } else {
        const wake = input.wake;
        if (wake.kind === "start" || request.waitID !== wake.waitID) return null;
        const before = restoreRunCore(request.proposal.before).getSnapshot();
        if (!before || before.state !== "suspended" || canonicalJson(resumeSnapshot(before, active.executionToken)) !== activeJSON) return null;
        const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=?", [wake.waitID, view.id])[0];
        if (!wait || wait.resumed_frame_id !== frame.id || wait.winner !== wake.kind || !sameJsonChunks(await readWaitWake(store, wait, view.id, recoveryEvidence), wake)) return null;
        if (!hasWaitResolutionEvents(store, view.id, wake.waitID, wake.facts, wake.kind === "timeout" ? wake.pendingActionKeys : undefined)) return null;
        const facts = store.query("SELECT fact_id FROM frame_facts WHERE frame_id=? AND run_id=? ORDER BY fact_id", [frame.id as string, view.id]).map(item => item.fact_id);
        if (canonicalJson(facts) !== canonicalJson(wake.facts.map(fact => fact.id).sort())) return null;
        for (const fact of wake.facts) {
          const saved = store.query("SELECT f.action_id,f.outcome_json,f.outcome_projection_id,a.action_key FROM facts f JOIN actions a ON a.id=f.action_id WHERE f.id=? AND f.run_id=?", [fact.id, view.id])[0];
          if (!saved || saved.action_id !== fact.actionID || saved.action_key !== fact.actionKey || !sameJsonChunks(await (recoveryEvidence ? readFactOutcomeForEvidence(store, saved) : readFactOutcome(store, saved)), fact.outcome)) return null;
        }
      }
      return stillAuthoritative() ? { active, proof, frame, driver } : null;
  }
  async function recoverActive(work: Row, row: Row, view: RunView): Promise<boolean> {
    let original: CommitBatch | undefined;
    let expected: { run: RunView; frame: Row; driver: Row } | undefined;
    try {
      const verified = await activeFrameEvidence(row, view, row.core_json as string);
      if (!verified) return false;
      const { active, proof, frame, driver } = verified;
      const after = suspendSnapshot(active, active.executionToken);
      const operationID = randomUUID(), proposal = { operationID, action: "suspend", before: active, after };
      const updated: RunView = { ...view, revision: view.revision + 1, updatedAt: now(), lifecycle: "suspended" };
      expected = { run: updated, frame, driver };
      original = { operationID, kind: "core.recovery_suspend", subjectID: view.id,
        request: json({ proposal, sourceOperationID: proof.operation_id, frameID: frame.id, oldCoordinatorEpoch: driver.coordinator_epoch }),
        writes: [...(work.stage === "cancel" ? [] : [cancellationGuard({ kind: "run", id: view.id })]), runWrite(updated, view, json(after), json(active)), stage(work.id as string, work.stage as string, work.stage as string),
          write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=? AND input_json=?", ["open", frame.id as string, view.id, "open", frame.input_json as string]),
          write("UPDATE driver_jobs SET state=? WHERE id=? AND run_id=? AND state=? AND coordinator_epoch=?", [driver.state as string, frame.id as string, view.id, driver.state as string, driver.coordinator_epoch as number])],
        evidence: json(proposal), result: null };
      noteRecoveryScope(view.id);
      store.commit(original);
      return true;
    } catch (error) {
      // A definite CAS loss can be retried from fresh authority. An uncertain
      // commit or invalid evidence must remain isolated.
      if (error instanceof HarnessError && error.info.disposition !== "uncertain") {
        if (error.info.code === "REVISION_CONFLICT") return true;
        if (error.info.code === "EXECUTION_FENCED" && store.effectiveCancellation({ kind: "run", id: view.id })) return true;
      }
      if (original && expected && isUncertain(error) && store.requiresRecovery && store.unresolvedCommit?.operationID === original.operationID) {
        try {
          const witness = workWitness, proposal = original.evidence as { before: Json; after: Json };
          const recovery = store.recoverAuthority();
          if (recovery.newEpoch !== store.coordinatorEpoch) return false;
          const proof = store.getCommit(original.operationID);
          const current = store.query("SELECT * FROM runs WHERE id=?", [view.id])[0];
          if (!current) return false;
          if (proof) {
            const record = store.query("SELECT request_json FROM history WHERE operation_id=?", [original.operationID])[0];
            const frame = store.query("SELECT * FROM frames WHERE id=? AND run_id=?", [decode<{ frameID: string }>(canonicalJson(original.request)).frameID, view.id])[0];
            const job = frame && store.query("SELECT * FROM driver_jobs WHERE id=? AND run_id=?", [frame.id as string, view.id])[0];
            const currentWork = store.query("SELECT stage FROM work_items WHERE id=? AND kind='run' AND subject_id=?", [work.id as string, view.id])[0];
            if (!record || proof.kind !== original.kind || proof.subjectID !== original.subjectID || record.request_json !== canonicalJson(original.request)
                || canonicalJson(proof.evidence) !== canonicalJson(original.evidence) || current.core_json !== canonicalJson(proposal.after)
                || !sameJsonChunks(readRunSnapshot(store, current.snapshot_json), expected.run) || current.revision !== expected.run.revision
                || !frame || canonicalJson(frame) !== canonicalJson(expected.frame) || !job || canonicalJson(job) !== canonicalJson(expected.driver)
                || currentWork?.stage !== work.stage) return false;
            store.finishRecovery(); return true;
          }
          if (witness === undefined || (!store.recoveryWitnessUnchanged(witness) && !recoveryScopeMatches(view.id))
              || current.core_json !== canonicalJson(proposal.before) || current.snapshot_json !== row.snapshot_json || current.revision !== row.revision) return false;
          // The old path is closed and its write lock has ended. Either nothing
          // else committed, or only unrelated rows moved. Retry the sealed
          // original batch, with its original operation ID and CAS conditions.
          store.finishRecovery(); store.commit(original); return true;
        } catch { return false; }
      }
      return false;
    }
  }
  function recoveredJobWrites(work: Row, row: Row, view: RunView, evidence: NonNullable<Awaited<ReturnType<typeof activeFrameEvidence>>>): SQLWrite[] | null {
      const { frame, driver } = evidence;
      if (!["pending", "assigned", "accepted"].includes(driver.state as string)) return null;
      const candidate = store.query("SELECT * FROM frame_candidates WHERE frame_id=?", [frame.id as string])[0];
      const finalization = store.query("SELECT * FROM finalize_jobs WHERE id=?", [frame.id as string])[0];
      if (work.stage === "driver") {
        if (candidate || finalization && driver.state !== "accepted") return null;
      } else {
        if (!candidate || driver.state !== "accepted" || typeof driver.report_json !== "string") return null;
        const report = readDriverValue(store, driver.report_json);
        if (!report.ok || !sameJsonChunks(validateDecision(report.decision), readFrameDecision(store, frame.id as string, candidate.decision_json))) return null;
        const records = store.query("SELECT request_json FROM history WHERE type='run.candidate' AND subject_id=?", [view.id]);
        const matching = records.filter(record => {
          const item = decode<{ frameID: string; decision: Json }>(record.request_json);
          return item.frameID === frame.id && canonicalJson(item.decision) === candidate.decision_json;
        });
        if (matching.length !== 1 || work.stage === "finalize" && !finalization) return null;
      }
      if (finalization && (finalization.run_id !== view.id || finalization.agent_id !== view.agent.id || finalization.agent_version !== view.agent.version
          || finalization.principal_id !== row.principal_id || frameJobInput(finalization, frame) === null || finalization.coordinator_epoch !== driver.coordinator_epoch)) return null;
      const lease = store.query("SELECT * FROM business_leases WHERE kind='run' AND subject_id=?", [view.id])[0];
      const oldState = driver.state as string;
      const jobWrite = oldState === "accepted"
        ? write("UPDATE driver_jobs SET coordinator_epoch=? WHERE id=? AND run_id=? AND state=? AND coordinator_epoch=? AND assignment_epoch=? AND report_json=? AND assignment_json=? AND assignment_encoding=? AND grant_json=?", [store.coordinatorEpoch, frame.id as string, view.id, oldState, driver.coordinator_epoch as number, driver.assignment_epoch as number, driver.report_json as string, driver.assignment_json as string, driver.assignment_encoding as string, driver.grant_json as string])
        : write("UPDATE driver_jobs SET coordinator_epoch=?,state=?,assignment_id=NULL,assignment_epoch=0,node_incarnation=NULL,assignment_deadline_at=NULL,assignment_json=NULL,assignment_encoding='inline',grant_json=NULL,lease_epoch=NULL WHERE id=? AND run_id=? AND state=? AND coordinator_epoch=? AND assignment_epoch=?", [store.coordinatorEpoch, "pending", frame.id as string, view.id, oldState, driver.coordinator_epoch as number, driver.assignment_epoch as number]);
      const writes = [jobWrite, stage(work.id as string, work.stage as string, work.stage as string), write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=? AND input_json=?", ["open", frame.id as string, view.id, "open", frame.input_json as string])];
      if (finalization) writes.push(write("UPDATE finalize_jobs SET coordinator_epoch=?,state=?,grant_json=NULL WHERE id=? AND run_id=? AND state=? AND coordinator_epoch=? AND assignment_epoch=?", [store.coordinatorEpoch, "pending", frame.id as string, view.id, finalization.state as string, finalization.coordinator_epoch as number, finalization.assignment_epoch as number]));
      if (lease) writes.push(write("UPDATE business_leases SET released=1 WHERE kind=? AND subject_id=? AND epoch=? AND released=? AND expires_at=?", ["run", view.id, lease.epoch as number, lease.released as number, lease.expires_at as number]));
      return writes;
  }
  async function resumeRecoveredFrame(work: Row, row: Row, view: RunView): Promise<boolean> {
    let core: RunCore | undefined;
    try {
      const bridge = store.query("SELECT * FROM history WHERE subject_id=? AND type IN ('core.start','core.resume','core.suspend','core.complete','core.recovery_suspend','core.recovery_resume') ORDER BY sequence DESC LIMIT 1", [view.id])[0];
      if (!bridge || bridge.type !== "core.recovery_suspend" || typeof bridge.operation_id !== "string" || typeof bridge.sequence !== "number") return false;
      const request = decode<{ proposal: { operationID: string; action: string; before: Json; after: Json }; sourceOperationID: string; frameID: string; oldCoordinatorEpoch: number }>(bridge.request_json);
      if (!request.proposal || request.proposal.operationID !== bridge.operation_id || request.proposal.action !== "suspend"
          || canonicalJson(request.proposal) !== bridge.data_json || canonicalJson(request.proposal.after) !== row.core_json) return false;
      const activeJSON = canonicalJson(request.proposal.before);
      const activeView = { ...view, lifecycle: (request.proposal.before as { state?: RunView["lifecycle"] }).state ?? null };
      const evidence = await activeFrameEvidence(row, activeView, activeJSON, bridge.sequence);
      if (!evidence || evidence.proof.operation_id !== request.sourceOperationID || evidence.frame.id !== request.frameID
          || evidence.driver.coordinator_epoch !== request.oldCoordinatorEpoch
          || canonicalJson(suspendSnapshot(evidence.active, evidence.active.executionToken)) !== row.core_json) return false;
      const { frame, driver } = evidence;
      const writes = recoveredJobWrites(work, row, view, evidence);
      if (!writes) return false;
      core = restoreRunCore(decode(row.core_json));
      const suspended = core.getSnapshot();
      if (!suspended || suspended.state !== "suspended" || suspended.runID !== view.id) return false;
      const activeCore = core;
      cores.set(view.id, activeCore);
      await transition(view.id, activeCore, () => activeCore.resume(async proposal => {
        const updated: RunView = { ...view, lifecycle: "resumed", revision: view.revision + 1, updatedAt: now() };
        try {
          commit(proposal.operationID, "core.recovery_resume", view.id, { proposal, frameID: frame.id, sourceBridgeID: bridge.operation_id, oldCoordinatorEpoch: driver.coordinator_epoch, coordinatorEpoch: store.coordinatorEpoch },
            [runWrite(updated, view, json(proposal.after), json(proposal.before)), ...writes], proposal);
          return "accepted";
        } catch (error) { return isUncertain(error) ? "unknown" : "rejected"; }
      }));
      return true;
    } catch (error) {
      if (error instanceof CoreError && error.code === "CONFIRMATION_REJECTED") return true;
      return false;
    } finally {
      if (core && core.getSnapshot()?.state === "suspended" && !quarantined.has(view.id)) cores.delete(view.id);
    }
  }
  async function process(work: Row) {
    const row = store.query("SELECT * FROM runs WHERE id=?", [work.subject_id as string])[0];
    if (!row) return;
    if (work.stage === "waiting") {
      const control = readRunControl(store, row.snapshot_json);
      if (!control.outcome && !control.blocked && !control.cancelRequested && !quarantined.has(control.id)) await resolveWait(work, control);
      return;
    }
    if (work.stage === "wake") {
      const control = readRunControl(store, row.snapshot_json);
      const job = control.wait && store.query("SELECT state FROM wake_jobs WHERE id=? AND run_id=?", [control.wait.id, control.id])[0];
      if (job && job.state !== "accepted") return;
    }
    const view = readRunSnapshot(store, row.snapshot_json), runID = view.id;
    if (work.stage === "cancel") { await cancelRun(work, row, view); return; }
    if (view.outcome || view.blocked || view.cancelRequested || quarantined.has(runID)) return;
    const liveCore = cores.get(runID);
    if (liveCore && ["driver", "dispatch_prepare", "validation", "complete", "finalize"].includes(work.stage as string)) {
      const job = store.query("SELECT coordinator_epoch FROM driver_jobs WHERE run_id=? AND id IN (SELECT id FROM frames WHERE run_id=? AND state='open')", [runID, runID])[0];
      if (job && job.coordinator_epoch !== store.coordinatorEpoch) {
        const active = liveCore.getSnapshot();
        const evidence = active && row.core_json === canonicalJson(active) ? await activeFrameEvidence(row, view, row.core_json as string) : null;
        const writes = evidence && recoveredJobWrites(work, row, view, evidence);
        if (!evidence || !writes) { quarantined.add(runID); return; }
        commit(randomUUID(), "run.rebind_after_reopen", runID, { frameID: evidence.frame.id, sourceOperationID: evidence.proof.operation_id,
          oldCoordinatorEpoch: evidence.driver.coordinator_epoch, coordinatorEpoch: store.coordinatorEpoch },
          [write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND core_json=?", [runID, view.revision, row.snapshot_json as string, row.core_json as string]), ...writes], null);
        return;
      }
    }
    if (["driver", "dispatch_prepare", "validation", "complete", "finalize"].includes(work.stage as string) && !cores.has(runID) && row.core_json) {
      if (view.lifecycle === "started" || view.lifecycle === "resumed") {
        if (!await recoverActive(work, row, view)) quarantined.add(runID);
        return;
      }
      if (view.lifecycle === "suspended") {
        if (!await resumeRecoveredFrame(work, row, view)) quarantined.add(runID);
        return;
      }
    }
    if (work.stage === "wake") {
      if (view.lifecycle !== "suspended" || !view.wait || !view.wait.winner || !row.core_json) return;
      const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=?", [view.wait.id, runID])[0];
      if (!wait || wait.winner !== view.wait.winner || !wait.wake_json || wait.resumed_frame_id !== null || wait.action_keys_json !== canonicalJson(view.wait.actionKeys) || wait.deadline_at !== view.wait.deadlineAt) return;
      const wake = await readWaitWake(store, wait, runID);
      if (wake.waitID !== view.wait.id || wake.kind !== view.wait.winner || !Array.isArray(wake.facts) || new Set(wake.facts.map(fact => fact.id)).size !== wake.facts.length) return;
      const byKey = new Map(wake.facts.map(fact => [fact.actionKey, fact]));
      if (byKey.size !== wake.facts.length || wake.facts.some(fact => !view.wait!.actionKeys.includes(fact.actionKey))) return;
      const pending = view.wait.actionKeys.filter(key => !byKey.has(key));
      if (wake.kind === "results" && pending.length || wake.kind === "timeout" && canonicalJson(wake.pendingActionKeys) !== canonicalJson(pending)) return;
      for (const fact of wake.facts) {
        const stored = store.query("SELECT f.id,f.action_id,f.outcome_json,f.outcome_projection_id,a.action_key FROM facts f JOIN actions a ON a.id=f.action_id WHERE f.id=? AND f.run_id=?", [fact.id, runID])[0];
        if (!stored || stored.action_id !== fact.actionID || stored.action_key !== fact.actionKey || !sameJsonChunks(await readFactOutcome(store, stored), fact.outcome)) return;
      }
      if (view.frameCount >= view.limits.maxFrames) { block(work, view, "LIMIT_EXCEEDED", "Frame limit reached", { maxFrames: view.limits.maxFrames }); return; }
      const frameID = randomUUID();
      const input: AdvanceInput = { frameID, run: { id: view.id, rootID: view.rootID, parentActionID: view.parentActionID, agent: view.agent, input: view.input, config: view.config, limits: view.limits }, continuation: view.continuation, wake };
      if (await frameExceedsBytes(input, options.maxFrameBytes)) { block(work, view, "LIMIT_EXCEEDED", "Frame exceeds size limit", { waitID: view.wait.id }); return; }
      const waitData = decode<Json>(wait.wake_json);
      const frameWitness = options.enableReferencedFrameInput || await frameExceedsBytes(wake, 16_384) ? await canonicalValueWitness(input) : null;
      const wakeJob = store.query("SELECT * FROM wake_jobs WHERE id=? AND run_id=?", [view.wait.id, runID])[0];
      if (!wakeJob) {
        const hasWakeReference = waitData && typeof waitData === "object" && !Array.isArray(waitData)
          && (waitData.encoding === "wait-fact-witness-v1" || waitData.encoding === "wait-fact-witness-v2");
        commit(randomUUID(), "run.wake_ready", runID, { waitID: view.wait.id, ...(hasWakeReference ? { wakeRef: waitData } : { wake }) }, [write("UPDATE waits SET winner=? WHERE id=? AND run_id=? AND winner=? AND wake_json=? AND resumed_frame_id IS NULL", [view.wait.winner, view.wait.id, runID, view.wait.winner, wait.wake_json as string]),
          stage(work.id as string, "wake", "wake"), write("INSERT INTO wake_jobs (id,run_id,agent_id,agent_version,principal_id,state) VALUES (?,?,?,?,?,?)", [view.wait.id, runID, view.agent.id, view.agent.version, row.principal_id as string, "pending"])], null);
        return;
      }
      if (wakeJob.state !== "accepted" || !wakeJob.assignment_json || !wakeJob.grant_json) return;
      const grant = decode<LeaseGrant>(wakeJob.grant_json);
      const assignment = decode<{ wake: boolean; jobID: string; waitID: string; runID: string; principalID: string; agent: RunView["agent"]; nodeID: string; incarnation: string; assignmentID: string; assignmentEpoch: number; lease: LeaseGrant }>(wakeJob.assignment_json);
      if (grant.kind !== "run" || grant.subjectID !== runID || assignment.wake !== true || assignment.jobID !== view.wait.id || assignment.waitID !== view.wait.id || assignment.runID !== runID
          || assignment.principalID !== row.principal_id || canonicalJson(assignment.agent) !== canonicalJson(view.agent) || assignment.nodeID !== grant.nodeID || assignment.incarnation !== grant.incarnation
          || assignment.assignmentID !== wakeJob.assignment_id || assignment.assignmentEpoch !== wakeJob.assignment_epoch || !assignment.lease || assignment.lease.epoch !== grant.epoch
          || assignment.lease.workerPrincipalID !== grant.workerPrincipalID || wakeJob.agent_id !== view.agent.id || wakeJob.agent_version !== view.agent.version
          || wakeJob.principal_id !== row.principal_id || wakeJob.node_incarnation !== grant.incarnation || wakeJob.lease_epoch !== grant.epoch) return;
      try { leases.assert(grant); } catch { return; }
      if (!workerAllowed(grant)) return;
      options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent);
      let core = cores.get(runID);
      if (!core) {
        try {
          core = restoreRunCore(decode(row.core_json));
          const snapshot = core.getSnapshot();
          if (!snapshot || snapshot.state !== "suspended" || snapshot.runID !== runID) throw new Error("Invalid suspended snapshot");
          cores.set(runID, core);
        } catch { quarantined.add(runID); return; }
      }
      const snapshot = core.getSnapshot();
      if (!snapshot || snapshot.state !== "suspended" || canonicalJson(snapshot) !== row.core_json) return;
      const activeCore = core, at = now();
      await transition(runID, activeCore, () => activeCore.resume(async proposal => {
        const updated: RunView = { ...view, lifecycle: "resumed", wait: null, frameCount: view.frameCount + 1, revision: view.revision + 1, updatedAt: at };
        const inputRef = frameWitness ? json({ encoding: "frame-wait-ref-v1", operationID: proposal.operationID,
          input: { ...(encodeRunFrame(store, input) as Record<string, Json>), wake: null }, waitID: view.wait!.id, ...frameWitness }) : null;
        const writes: SQLWrite[] = [...leases.guard(grant), write("UPDATE wake_jobs SET state=? WHERE id=? AND run_id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND lease_epoch=? AND assignment_json=? AND grant_json=?", ["accepted", view.wait!.id, runID, "accepted", wakeJob.assignment_id as string, wakeJob.assignment_epoch as number, grant.incarnation, grant.epoch, wakeJob.assignment_json as string, wakeJob.grant_json as string]), releaseGrant(grant),
          runWrite(updated, view, json(proposal.after), json(proposal.before)),
          write("INSERT INTO frames (id,run_id,number,input_json,state,input_encoding,input_operation_id) VALUES (?,?,?,?,?,?,?)", [frameID, runID, updated.frameCount, { json: inputRef ?? encodeRunFrame(store, input) }, "open", inputRef ? "wait-ref-v1" : "inline", inputRef ? proposal.operationID : null]),
          write("UPDATE waits SET resumed_frame_id=? WHERE id=? AND run_id=? AND winner=? AND wake_json=? AND resumed_frame_id IS NULL", [frameID, view.wait!.id, runID, view.wait!.winner!, wait.wake_json as string]),
          write("INSERT INTO driver_jobs (id,run_id,agent_id,agent_version,input_json,principal_id,state,coordinator_epoch,input_encoding) VALUES (?,?,?,?,?,?,?,?,?)", [frameID, runID, view.agent.id, view.agent.version, { json: null }, row.principal_id as string, "pending", store.coordinatorEpoch, "frame-ref-v1"]),
          stage(work.id as string, "wake", "driver")];
        for (const fact of wake.facts) writes.push(write("INSERT INTO frame_facts(frame_id,fact_id,run_id) VALUES(?,?,?)", [frameID, fact.id, runID]));
        try {
          commit(proposal.operationID, "core.resume", runID, { proposal, frameID, ...(inputRef ? { inputRef } : { input: encodeRunFrame(store, input) }), waitID: view.wait!.id, grant, wakeAssignment: assignment }, writes, proposal,
            [{ type: "run.frame_created", subjectID: runID, data: json({ frameID, frameCount: updated.frameCount }) }], grant.expiresAt);
          return "accepted";
        } catch (error) { return isUncertain(error) ? "unknown" : "rejected"; }
      }));
      return;
    }
    if (work.stage === "start") {
      if (view.lifecycle !== null || row.core_json !== null) return;
      if (view.frameCount >= view.limits.maxFrames) { block(work, view, "LIMIT_EXCEEDED", "Frame limit reached", { maxFrames: view.limits.maxFrames }); return; }
      const start = store.query("SELECT * FROM start_jobs WHERE id=?", [runID])[0];
      if (!start) {
        commit(randomUUID(), "run.start_ready", runID, { runID, agent: view.agent }, [
          write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND core_json IS NULL", [runID, view.revision, row.snapshot_json as string]),
          stage(work.id as string, "start", "start"),
          write("INSERT INTO start_jobs (id,agent_id,agent_version,principal_id,coordinator_epoch,state) VALUES (?,?,?,?,?,?)", [runID, view.agent.id, view.agent.version, row.principal_id as string, store.coordinatorEpoch, "pending"]),
        ], null);
        return;
      }
      if (start.coordinator_epoch !== store.coordinatorEpoch) {
        if (typeof start.coordinator_epoch !== "number" || start.coordinator_epoch >= store.coordinatorEpoch || start.agent_id !== view.agent.id
            || start.agent_version !== view.agent.version || start.principal_id !== row.principal_id) { quarantined.add(runID); return; }
        const lease = store.query("SELECT * FROM business_leases WHERE kind='run' AND subject_id=?", [runID])[0];
        const writes: SQLWrite[] = [write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND core_json IS NULL", [runID, view.revision, row.snapshot_json as string]),
          stage(work.id as string, "start", "start"),
          write("UPDATE start_jobs SET coordinator_epoch=?,state=?,assignment_id=NULL,node_incarnation=NULL,assignment_deadline_at=NULL,assignment_json=NULL,grant_json=NULL,lease_epoch=NULL WHERE id=? AND state=? AND coordinator_epoch=? AND assignment_epoch=?", [store.coordinatorEpoch, "pending", runID, start.state as string, start.coordinator_epoch, start.assignment_epoch as number])];
        if (lease) writes.push(write("UPDATE business_leases SET released=1 WHERE kind=? AND subject_id=? AND epoch=? AND released=? AND expires_at=?", ["run", runID, lease.epoch as number, lease.released as number, lease.expires_at as number]));
        commit(randomUUID(), "run.start_rebind", runID, { runID, oldCoordinatorEpoch: start.coordinator_epoch, coordinatorEpoch: store.coordinatorEpoch }, writes, null);
        return;
      }
      if (now() >= (work.available_at as number) + options.prepareTimeoutMs) {
        const at = now();
        const online = store.query("SELECT d.incarnation FROM node_definitions d JOIN node_sessions s ON s.incarnation=d.incarnation WHERE d.kind=? AND d.id=? AND d.version=? AND s.state=? AND s.expires_at>? LIMIT 1", ["agent", view.agent.id, view.agent.version, "online", at]);
        if (!online.length) {
          block(work, view, "VERSION_UNAVAILABLE", "Fixed Agent version unavailable", { phase: "start", retryAfterMs: options.retryAfterMs }, [write("UPDATE start_jobs SET state=? WHERE id=? AND state=? AND assignment_epoch=?", [start.state as string, runID, start.state as string, start.assignment_epoch as number])], undefined,
            [{ table: "online_definition", kind: "agent", id: view.agent.id, version: view.agent.version, at }]);
          return;
        }
      }
      if (start.state !== "accepted" || !start.assignment_json || !start.grant_json) return;
      const grant = decode<LeaseGrant>(start.grant_json);
      const assignment = decode<{ start: boolean; jobID: string; runID: string; principalID: string; agent: RunView["agent"]; nodeID: string; incarnation: string; assignmentID: string; assignmentEpoch: number; lease: LeaseGrant }>(start.assignment_json);
      if (grant.kind !== "run" || grant.subjectID !== runID || assignment.start !== true || assignment.jobID !== runID || assignment.runID !== runID
          || assignment.principalID !== row.principal_id || canonicalJson(assignment.agent) !== canonicalJson(view.agent) || assignment.nodeID !== grant.nodeID
          || assignment.incarnation !== grant.incarnation || assignment.assignmentID !== start.assignment_id || assignment.assignmentEpoch !== start.assignment_epoch
          || !assignment.lease || assignment.lease.epoch !== grant.epoch || assignment.lease.workerPrincipalID !== grant.workerPrincipalID
          || start.agent_id !== view.agent.id || start.agent_version !== view.agent.version || start.principal_id !== row.principal_id
          || start.node_incarnation !== grant.incarnation || start.lease_epoch !== grant.epoch || start.coordinator_epoch !== store.coordinatorEpoch) return;
      try { leases.assert(grant); } catch { return; }
      if (!workerAllowed(grant)) return;
      options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent);
      const frameID = randomUUID();
      const input: AdvanceInput = { frameID, run: { id: view.id, rootID: view.rootID, parentActionID: view.parentActionID, agent: view.agent, input: view.input, config: view.config, limits: view.limits }, continuation: view.continuation, wake: { kind: "start" } };
      const exceedsFrame = await frameExceedsBytes(input, options.maxFrameBytes);
      try { leases.assert(grant); } catch { return; }
      if (!workerAllowed(grant)) return;
      options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent);
      if (exceedsFrame) { block(work, view, "LIMIT_EXCEEDED", "Frame exceeds size limit", { frameID }); return; }
      const at = now();
      const core = createRunCore(runID); cores.set(runID, core);
      await transition(runID, core, () => core.start(async proposal => {
        const updated: RunView = { ...view, lifecycle: proposal.after.state, frameCount: view.frameCount + 1, revision: view.revision + 1, updatedAt: at };
        try {
          commit(proposal.operationID, "core.start", runID, { proposal, frameID, input: encodeRunFrame(store, input), grant, startAssignment: assignment }, [...leases.guard(grant),
            write("UPDATE start_jobs SET state=? WHERE id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND node_incarnation=? AND lease_epoch=? AND assignment_json=? AND grant_json=?", ["accepted", runID, "accepted", start.assignment_id as string, start.assignment_epoch as number, grant.incarnation, grant.epoch, start.assignment_json as string, start.grant_json as string]), releaseGrant(grant),
            runWrite(updated, view, json(proposal.after), json(proposal.before)),
            write("INSERT INTO frames (id,run_id,number,input_json,state) VALUES (?,?,?,?,?)", [frameID, runID, updated.frameCount, { json: encodeRunFrame(store, input) }, "open"]),
            write("INSERT INTO driver_jobs (id,run_id,agent_id,agent_version,input_json,principal_id,state,coordinator_epoch,input_encoding) VALUES (?,?,?,?,?,?,?,?,?)", [frameID, runID, view.agent.id, view.agent.version, { json: null }, row.principal_id as string, "pending", store.coordinatorEpoch, "frame-ref-v1"]),
            write("UPDATE work_items SET stage=?,available_at=? WHERE id=? AND stage=?", ["driver", at, work.id as string, "start"])], proposal,
            [{ type: "run.frame_created", subjectID: runID, data: json({ frameID, frameCount: updated.frameCount }) }], grant.expiresAt);
          return "accepted";
        } catch (error) { return isUncertain(error) ? "unknown" : "rejected"; }
      }));
      return;
    }
    // A preparation deadline is durable even when an active Core handle cannot
    // be reconstructed in this phase. Expiry only blocks and retains evidence.
    if (["dispatch_prepare", "validation", "finalize"].includes(work.stage as string)) {
      const expired = store.query("SELECT r.id,r.phase,r.deadline_at FROM prepare_rounds r JOIN frames f ON f.id=r.owner_id WHERE r.owner_kind='frame' AND r.state='pending' AND f.run_id=? AND f.state='open' AND r.deadline_at<=? ORDER BY r.deadline_at LIMIT 1", [runID, now()])[0];
      if (expired) {
        block(work, view, "VERSION_UNAVAILABLE", "Preparation expired", json({ phase: expired.phase, roundID: expired.id, deadlineAt: expired.deadline_at, retryAfterMs: options.retryAfterMs }),
          [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at=?", ["expired", expired.id as string, "pending", expired.deadline_at as number])]);
        return;
      }
    }
    // No reconstruction of a lost active handle in T06.
    const core = cores.get(runID);
    if (!core) return;
    const frame = store.query("SELECT * FROM frames WHERE run_id=? AND state=? ORDER BY number LIMIT 1", [runID, "open"])[0];
    if (!frame) return;
    if (work.stage === "driver") {
      const job = store.query("SELECT * FROM driver_jobs WHERE id=?", [frame.id as string])[0];
      if (!job || job.state !== "accepted") {
        if (job && (job.state === "pending" || job.state === "assigned") && now() >= (work.available_at as number) + options.prepareTimeoutMs) {
          const online = store.query("SELECT d.incarnation FROM node_definitions d JOIN node_sessions s ON s.incarnation=d.incarnation WHERE d.kind=? AND d.id=? AND d.version=? AND s.state=? AND s.expires_at>? LIMIT 1", ["agent", view.agent.id, view.agent.version, "online", now()]);
          if (!online.length) block(work, view, "VERSION_UNAVAILABLE", "Fixed Agent version unavailable", { phase: "driver", retryAfterMs: options.retryAfterMs }, [write("UPDATE driver_jobs SET state=? WHERE id=? AND state=? AND assignment_epoch=?", ["superseded", frame.id as string, job.state as string, job.assignment_epoch as number])], undefined,
            [{ table: "online_definition", kind: "agent", id: view.agent.id, version: view.agent.version, at: now() }]);
        }
        return;
      }
      if (job.run_id !== runID || job.agent_id !== view.agent.id || job.agent_version !== view.agent.version || job.principal_id !== row.principal_id
          || frameJobInput(job, frame) === null || job.coordinator_epoch !== store.coordinatorEpoch) return;
      if (!job.grant_json || !job.assignment_json) return;
      const driverGrant = decode<LeaseGrant>(job.grant_json);
      const driverAssignment = await readFrameAssignment(store, job);
      if (!("driver" in driverAssignment)) return;
      if (driverGrant.kind !== "run" || driverGrant.subjectID !== runID || driverAssignment.driver !== true || driverAssignment.jobID !== frame.id || driverAssignment.runID !== runID
          || driverAssignment.nodeID !== driverGrant.nodeID || driverAssignment.incarnation !== driverGrant.incarnation || driverAssignment.lease.epoch !== driverGrant.epoch
          || driverAssignment.lease.workerPrincipalID !== driverGrant.workerPrincipalID || driverAssignment.assignmentID !== job.assignment_id || driverAssignment.assignmentEpoch !== job.assignment_epoch
          || job.node_incarnation !== driverGrant.incarnation) return;
      let activeGrant: LeaseGrant | null = driverGrant;
      let finalization: Row | undefined;
      try { leases.assert(driverGrant); } catch { activeGrant = null; }
      if (!activeGrant) {
        finalization = store.query("SELECT * FROM finalize_jobs WHERE id=? AND run_id=?", [frame.id as string, runID])[0];
        if (!finalization) {
          commit(randomUUID(), "run.candidate_reclaim_ready", runID, { frameID: frame.id, report: decode(job.report_json) }, [
            write("UPDATE driver_jobs SET state=? WHERE id=? AND state=? AND report_json=? AND assignment_json=? AND assignment_encoding=? AND grant_json=?", ["accepted", frame.id as string, "accepted", job.report_json as string, job.assignment_json as string, job.assignment_encoding as string, job.grant_json as string]),
            write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=? AND input_json=?", ["open", frame.id as string, runID, "open", frame.input_json as string]),
            stage(work.id as string, "driver", "driver"), insertFinalization(frame, view, row.principal_id as string),
          ], null);
          return;
        }
        activeGrant = await finalizationGrant(finalization, frame, view, row.principal_id as string);
        if (!activeGrant) return;
      }
      const effectiveGrant = activeGrant;
      if (!workerAllowed(effectiveGrant)) return;
      options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent);
      const grantGuards = () => [...leases.guard(effectiveGrant), write("UPDATE driver_jobs SET state=? WHERE id=? AND state=? AND grant_json=? AND assignment_json=? AND assignment_encoding=? AND report_json=?", ["accepted", frame.id as string, "accepted", job.grant_json as string, job.assignment_json as string, job.assignment_encoding as string, job.report_json as string]), ...(finalization ? [finalizeGuard(finalization, effectiveGrant)] : []), releaseGrant(effectiveGrant)];
      const report = await readDriverValueAsync(store, job.report_json);
      if (!report.ok) {
        const issue = report.issue as { code?: string; details?: { phase?: string } } | undefined;
        const executionCode = issue?.details?.phase === "execution" && ["FORBIDDEN", "VERSION_UNAVAILABLE", "STATE_CONFLICT", "INVALID_ARGUMENT"].includes(issue.code ?? "") ? issue!.code! : "INVALID_DECISION";
        block(work, view, executionCode, executionCode === "INVALID_DECISION" ? "Driver failed" : "Agent execution checks failed", json({ issue: report.issue ?? null, frameID: frame.id }), grantGuards(), effectiveGrant.expiresAt); return;
      }
      let decision: Decision;
      try { decision = validateDecision(report.decision); }
      catch { block(work, view, "INVALID_DECISION", "Driver returned an invalid decision", json({ frameID: frame.id }), grantGuards(), effectiveGrant.expiresAt); return; }
      if (decision.kind === "dispatch") {
        if (await frameExceedsBytes(decision.continuation.state, options.maxValueBytes)) {
          block(work, view, "LIMIT_EXCEEDED", "Continuation state exceeds the size limit", json({ frameID: frame.id, maxValueBytes: options.maxValueBytes }), grantGuards(), effectiveGrant.expiresAt); return;
        }
        const actionKeys = decision.actions.map(action => action.key), waiting = decision.wait.all;
        if (new Set(actionKeys).size !== actionKeys.length || waiting.length === 0 || new Set(waiting).size !== waiting.length || actionKeys.some(key => !waiting.includes(key))) {
          block(work, view, "INVALID_DECISION", "Invalid dispatch action keys or wait set", json({ frameID: frame.id }), grantGuards(), effectiveGrant.expiresAt); return;
        }
      }
      const at = now(), roundID = randomUUID(), deadline = at + options.prepareTimeoutMs;
      const writes: SQLWrite[] = [...grantGuards(), write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=? AND input_json=?", ["open", frame.id as string, runID, "open", frame.input_json as string]),
        write("UPDATE driver_jobs SET state=? WHERE id=? AND state=? AND report_json=? AND run_id=? AND agent_id=? AND agent_version=? AND principal_id=? AND input_json=? AND input_encoding=? AND coordinator_epoch=?", ["accepted", frame.id as string, "accepted", job.report_json as string, runID, view.agent.id, view.agent.version, row.principal_id as string, job.input_json as string, job.input_encoding as string, store.coordinatorEpoch]),
        write("INSERT INTO frame_candidates (frame_id,decision_json,created_at) VALUES (?,?,?)", [frame.id as string, { json: driverDecisionStorage(job.report_json, decision) }, at])];
      if (decision.kind === "dispatch") {
        let issue: { code: string; message: string } | undefined;
        const input = await readFrameInput(store, frame);
        const pending = input.wake.kind === "timeout" ? input.wake.pendingActionKeys : [];
        const keys = new Set([...decision.actions.map(action => action.key), ...decision.wait.all]);
        const existing = new Map<string, Row>();
        for (const key of keys) {
          const found = store.query("SELECT * FROM actions WHERE run_id=? AND action_key=?", [runID, key])[0];
          if (found) existing.set(key, found);
        }
        const fresh: ActionRequest[] = [];
        if (!issue && pending.some(key => !decision.wait.all.includes(key))) issue = { code: "INVALID_DECISION", message: "Wait does not cover the frame's pending actions" };
        for (const action of decision.actions) {
          const previous = existing.get(action.key);
          if (!previous) { fresh.push(action); continue; }
          const snapshot = previous.snapshot_projection_id === null && previous.snapshot_json ? (expandActionRequests(store, decode<Json>(previous.snapshot_json)) as unknown as { original: ActionRequest }) : null;
          const original = previous.snapshot_projection_id === null
            ? snapshot?.original ?? readActionAcceptance(store, previous.id as string).original as unknown as ActionRequest
            : (await readActionReuseIdentity(store, previous)).original;
          if (!sameJsonChunks(original, action)) issue = { code: "INVALID_DECISION", message: "An existing Action key has a different original request" };
        }
        // A referenced identity can take multiple reads. Recheck the original
        // worker and caller after every such await, before committing a result.
        if (!workerAllowed(effectiveGrant)) return;
        try { leases.assert(effectiveGrant); options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent); }
        catch { return; }
        const newKeys = new Set(fresh.map(action => action.key));
        if (!issue && decision.wait.all.some(key => !newKeys.has(key) && !existing.has(key))) issue = { code: "INVALID_DECISION", message: "Wait references an unknown Action key" };
        if (!issue && readRunSnapshot(store, store.query("SELECT snapshot_json FROM runs WHERE id=?", [view.rootID])[0]!.snapshot_json).rootActionCount + fresh.length > view.limits.maxActions) issue = { code: "LIMIT_EXCEEDED", message: "Action limit reached" };
        if (issue) {
          const blocked: RunView = { ...view, revision: view.revision + 1, updatedAt: at, blocked: { ...issue, details: json({ frameID: frame.id }) } };
          writes.push(runWrite(blocked, view), stage(work.id as string, "driver", "blocked"));
          commit(randomUUID(), "run.candidate", runID, { frameID: frame.id, decision: driverDecisionStorage(job.report_json, decision) }, writes, null, [{ type: "run.blocked", subjectID: runID, data: json({ blocked: blocked.blocked }) }], effectiveGrant.expiresAt); return;
        }
        const owner = store.query("SELECT configuration_json FROM node_sessions WHERE node_id=? AND incarnation=?", [driverAssignment.nodeID, driverAssignment.incarnation])[0];
        if (!owner) return;
        const configured = (readNodeConfiguration(store, owner.configuration_json) as unknown as { policies: AppliedPolicy[] }).policies;
        const selected = policiesForRun(configured, view.agent);
        const policies = selected.policies, policySource = selected.scoped ? null : storedPolicySource(owner.configuration_json);
        const rewrite = fresh.length > 0 && policies.length > 0;
        const originalRecord = encodePreparationOriginals(frame.id as string, decision, fresh);
        const payload = { kind: "agent", ref: view.agent, phase: "rewrite", value: fresh, config: null, principalID: row.principal_id,
          operation: "client.submit", context: { domainID: store.domainID, rootID: view.rootID, runID, frameID: frame.id, decision, policies, ...(policySource ? { policySource } : {}) } };
        writes.push(write("INSERT INTO prepare_rounds (id,owner_kind,owner_id,principal_id,phase,number,prepare_timeout_ms,created_at,deadline_at,state) VALUES (?,?,?,?,?,?,?,?,?,?)", [roundID, "frame", frame.id as string, row.principal_id as string, "dispatch", 1, options.prepareTimeoutMs, at, deadline, "pending"]),
          write("INSERT INTO dispatch_preparations (frame_id,round_id,policies_json,original_json,rewritten_json,state) VALUES (?,?,?,?,?,?)", [frame.id as string, roundID, { json: encodePreparationPolicies(policies, policySource) }, { json: originalRecord }, rewrite ? null : { json: originalRecord }, rewrite ? "rewrite" : "resolve"]),
          stage(work.id as string, "driver", "dispatch_prepare"));
        if (rewrite) for (const action of fresh) writes.push(write("INSERT INTO validation_jobs (id,round_id,payload_json,state) VALUES (?,?,?,?)", [randomUUID(), roundID, { json: compactDispatchPayload({ ...payload, value: [action] }) }, "pending"]));
        commit(randomUUID(), "run.candidate", runID, { frameID: frame.id, decision: driverDecisionStorage(job.report_json, decision), policies: encodePreparationPolicies(policies, policySource), fresh: originalRecord }, writes, null, [], effectiveGrant.expiresAt); return;
      }
      if (decision.outcome.kind === "succeeded") {
        const payload = agentOutputValidation(store, view, frame.id as string, row.principal_id as string, decision);
        writes.push(write("INSERT INTO prepare_rounds (id,owner_kind,owner_id,principal_id,phase,number,prepare_timeout_ms,created_at,deadline_at,state) VALUES (?,?,?,?,?,?,?,?,?,?)", [roundID, "frame", frame.id as string, row.principal_id as string, "output", 1, options.prepareTimeoutMs, at, deadline, "pending"]),
          write("INSERT INTO validation_jobs (id,round_id,payload_json,state) VALUES (?,?,?,?)", [randomUUID(), roundID, { json: compactDispatchPayload(payload) }, "pending"]), stage(work.id as string, "driver", "validation"));
      } else writes.push(stage(work.id as string, "driver", "complete"));
      commit(randomUUID(), "run.candidate", runID, { frameID: frame.id, decision: driverDecisionStorage(job.report_json, decision) }, writes, null, [], effectiveGrant.expiresAt); return;
    }
    const candidate = store.query("SELECT * FROM frame_candidates WHERE frame_id=?", [frame.id as string])[0];
    if (!candidate) return;
    const decision = readFrameDecision(store, frame.id as string, candidate.decision_json);
    if (decision.kind === "dispatch") {
      if (work.stage !== "dispatch_prepare" && work.stage !== "finalize") return;
      const preparation = store.query("SELECT * FROM dispatch_preparations WHERE frame_id=?", [frame.id as string])[0];
      const round = preparation && store.query("SELECT * FROM prepare_rounds WHERE id=? AND state=?", [preparation.round_id as string, "pending"])[0];
      if (!preparation || !round) return;
      const at = now(), roundID = round.id as string, deadline = round.deadline_at as number;
      if (at >= deadline) {
        block(work, view, "VERSION_UNAVAILABLE", "Dispatch preparation expired", json({ phase: "dispatch", roundID, deadlineAt: deadline, retryAfterMs: options.retryAfterMs }),
          [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at=?", ["expired", roundID, "pending", deadline])]);
        return;
      }
      // Pending pure work needs only fixed routing/revision metadata. Avoid
      // reconstructing the same large configuration on every polling pass.
      if (preparation.state === "rewrite" && store.query("SELECT id FROM validation_jobs WHERE round_id=? AND state NOT IN ('accepted','superseded') LIMIT 1", [roundID])[0]) return;
      const waitingValidation = preparation.state === "validation" && store.query("SELECT id FROM validation_jobs WHERE round_id=? AND state NOT IN ('accepted','superseded') LIMIT 1", [roundID])[0];
      const waitingFinalization = preparation.state === "ready" && work.stage === "finalize" && !store.query("SELECT id FROM finalize_jobs WHERE id=? AND state='accepted'", [frame.id as string])[0];
      if (waitingValidation || waitingFinalization) {
        const revisions = decode<(number | null)[]>(preparation.pointer_revisions_json), rewritten = readPreparationRewritten(store, preparation), record = decode<any>(preparation.resolved_json);
        const versions: string[] = Array.isArray(record) ? record.map(action => action.target.version) : record.versions;
        if (Array.isArray(versions) && versions.length === rewritten.length && revisions.length === rewritten.length && !rewritten.some((action, index) => {
          if (revisions[index] === null) return false;
          const pointer = store.query("SELECT version,revision FROM version_pointers WHERE kind=? AND id=?", [action.kind, action.target.id])[0];
          return !pointer || pointer.version !== versions[index] || pointer.revision !== revisions[index];
        })) return;
      }
      const policies = readPreparationPolicies(store, preparation), policySource = storedPolicySource(preparation.policies_json);
      const original = readPreparationOriginals(store, preparation);
      if (preparation.state === "rewrite") {
        // New jobs carry one Action; persisted legacy jobs may carry the full batch.
        // No part of the frame advances until every original has an accepted rewrite.
        const rewriteJobs = store.query("SELECT * FROM validation_jobs WHERE round_id=? AND state<>? ORDER BY id", [roundID, "superseded"])
          .filter(item => decode<{ phase: string }>(item.payload_json).phase === "rewrite");
        if (!rewriteJobs.length || rewriteJobs.some(job => job.state !== "accepted")) return;
        const byKey = new Map(original.map(action => [action.key, action])), byRewritten = new Map<string, ActionRequest>();
        let valid = true;
        for (const job of rewriteJobs) {
          const payload = decode<{ value: ActionRequest[] }>(job.payload_json);
          if (!Array.isArray(payload.value)) return;
          const selected = payload.value.map(action => byKey.get(action.key));
          if (selected.some(action => !action)) return;
          const expected = { kind: "agent", ref: view.agent, phase: "rewrite", value: selected, config: null, principalID: row.principal_id,
            operation: "client.submit", context: { domainID: store.domainID, rootID: view.rootID, runID, frameID: frame.id, decision, policies } };
          if (!dispatchPayloadMatches(expected, job.payload_json)) return;
          const conclusion = await readValidationConclusionAsync(store, job.report_json);
          if (!conclusion.valid || !Array.isArray(conclusion.rewritten) || conclusion.rewritten.length !== selected.length) { valid = false; break; }
          try {
            for (let index = 0; index < selected.length; index++) {
              const action = validateActionRequest(conclusion.rewritten[index]), source = selected[index]!;
              if (action.key !== source.key || action.kind !== source.kind || byRewritten.has(action.key)) { valid = false; break; }
              byRewritten.set(action.key, action);
            }
          } catch { valid = false; }
          if (!valid) break;
        }
        let rewritten = original.map(action => byRewritten.get(action.key)!);
        valid = valid && byRewritten.size === original.length;
        if (valid) {
          try {
            const checked = validateDecision({ ...decision, actions: rewritten });
            if (checked.kind !== "dispatch") valid = false;
            else rewritten = checked.actions;
          } catch { valid = false; }
        }
        const jobGuards = rewriteJobs.map(job => write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND payload_json=? AND report_json=? AND assignment_id=? AND assignment_epoch=?",
          ["accepted", job.id as string, "accepted", job.payload_json as string, job.report_json as string, job.assignment_id as string, job.assignment_epoch as number]));
        if (!valid) {
          block(work, view, "INVALID_DECISION", "Action rewrite was rejected or changed identity", json({ frameID: frame.id }),
            [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at]), ...jobGuards], deadline);
          return;
        }
        const rewrittenRecord = encodePreparationRewrites(preparation, rewriteJobs, rewritten);
        commit(randomUUID(), "run.rewrite_prepared", runID, { frameID: frame.id, roundID, rewritten: rewrittenRecord }, [
          write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", roundID, "pending", at]), ...jobGuards,
          write("UPDATE dispatch_preparations SET state=?,rewritten_json=? WHERE frame_id=? AND round_id=? AND state=?", ["resolve", { json: rewrittenRecord }, frame.id as string, roundID, "rewrite"]),
        ], null, [], deadline);
        return;
      }
      if (preparation.state === "resolve") {
        const rewritten = readPreparationRewritten(store, preparation);
        const resolved: import("../contract/index.js").PreparedActionRequest[] = [];
        const revisions: (number | null)[] = [];
        for (const action of rewritten) {
          if (Buffer.byteLength(canonicalJson(action.input), "utf8") > options.maxValueBytes || Buffer.byteLength(canonicalJson(action.config), "utf8") > options.maxValueBytes) {
            block(work, view, "LIMIT_EXCEEDED", "Action input or configuration exceeds the size limit", json({ frameID: frame.id, key: action.key }), [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at])], deadline);
            return;
          }
          let version = action.target.version, revision: number | null = null;
          try { options.authorizePrincipal(row.principal_id as string, "client.submit", action.kind, { id: action.target.id }); }
          catch {
            block(work, view, "FORBIDDEN", "Action target is not authorized", json({ frameID: frame.id, key: action.key }), [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at])], deadline);
            return;
          }
          if (version === undefined) {
            const pointer = store.query("SELECT version,revision FROM version_pointers WHERE kind=? AND id=?", [action.kind, action.target.id])[0];
            if (!pointer) {
              block(work, view, "VERSION_UNAVAILABLE", "Action has no default version", json({ frameID: frame.id, key: action.key }), [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at])], deadline,
                [{ table: "version_pointers", kind: action.kind, id: action.target.id }]);
              return;
            }
            version = pointer.version as string; revision = pointer.revision as number;
          }
          try { options.authorizePrincipal(row.principal_id as string, "client.submit", action.kind, { id: action.target.id, version }); }
          catch {
            block(work, view, "FORBIDDEN", "Action target is not authorized", json({ frameID: frame.id, key: action.key }), [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at])], deadline);
            return;
          }
          if (!store.query("SELECT kind FROM definition_catalog WHERE kind=? AND id=? AND version=?", [action.kind, action.target.id, version]).length) {
            const guards = [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at])];
            if (revision !== null) guards.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [revision, action.kind, action.target.id, version, revision]));
            block(work, view, "VERSION_UNAVAILABLE", "Action target version is unknown", json({ frameID: frame.id, key: action.key }), guards, deadline,
              [{ table: "definition_catalog", kind: action.kind, id: action.target.id, version }]);
            return;
          }
          resolved.push({ ...action, target: { id: action.target.id, version }, policies }); revisions.push(revision);
        }
        const resolvedRecord = encodePreparationResolved(preparation, resolved);
        const writes: SQLWrite[] = [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", roundID, "pending", at]),
          write("UPDATE dispatch_preparations SET state=?,resolved_json=?,pointer_revisions_json=?,generation=generation+1 WHERE frame_id=? AND round_id=? AND state=? AND generation=?", [resolved.length ? "validation" : "ready", { json: resolvedRecord }, { json: json(revisions) }, frame.id as string, roundID, "resolve", preparation.generation as number])];
        for (const [index, prepared] of resolved.entries()) {
          const revision = revisions[index];
          if (revision !== null) writes.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [revision!, prepared.kind, prepared.target.id, prepared.target.version, revision!]));
          const payload = { kind: prepared.kind, ref: prepared.target, phase: "admission", value: prepared.input, config: prepared.config, principalID: row.principal_id,
            operation: "client.submit", context: { domainID: store.domainID, rootID: view.rootID, runID, frameID: frame.id, decision, policies, ...(policySource ? { policySource } : {}), original: original[index], prepared, pointerRevision: revision } };
          writes.push(write("INSERT INTO validation_jobs (id,round_id,payload_json,state) VALUES (?,?,?,?)", [randomUUID(), roundID, { json: compactDispatchPayload(payload) }, "pending"]));
        }
        commit(randomUUID(), "run.dispatch_resolved", runID, { frameID: frame.id, roundID, resolved: resolvedRecord, revisions, generation: preparation.generation }, writes, null, [], deadline);
        return;
      }
      if (preparation.state === "validation") {
        const rewritten = readPreparationRewritten(store, preparation);
        const resolved = readPreparationResolved(store, preparation);
        const revisions = decode<(number | null)[]>(preparation.pointer_revisions_json);
        const jobs = store.query("SELECT * FROM validation_jobs WHERE round_id=? AND state<>?", [roundID, "superseded"]);
        const admission = jobs.filter(item => decode<{ phase: string }>(item.payload_json).phase === "admission");
        const changed = resolved.some((request, index) => {
          if (revisions[index] === null) return false;
          const pointer = store.query("SELECT version,revision FROM version_pointers WHERE kind=? AND id=?", [request.kind, request.target.id])[0];
          return !pointer || pointer.version !== request.target.version || pointer.revision !== revisions[index];
        });
        if (changed) {
          const writes: SQLWrite[] = [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", roundID, "pending", at]),
            write("UPDATE dispatch_preparations SET state=?,resolved_json=NULL,pointer_revisions_json=NULL WHERE frame_id=? AND round_id=? AND state=? AND generation=?", ["resolve", frame.id as string, roundID, "validation", preparation.generation as number])];
          for (const job of admission) writes.push(write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND assignment_epoch=?", ["superseded", job.id as string, job.state as string, job.assignment_epoch as number]));
          commit(randomUUID(), "run.dispatch_reprepare", runID, { frameID: frame.id, roundID, generation: preparation.generation }, writes, null, [], deadline);
          return;
        }
        if (admission.length !== resolved.length || admission.some(job => job.state !== "accepted")) return;
        const matched: Row[] = [];
        for (const [index, prepared] of resolved.entries()) {
          const payload = { kind: prepared.kind, ref: prepared.target, phase: "admission", value: prepared.input, config: prepared.config, principalID: row.principal_id,
            operation: "client.submit", context: { domainID: store.domainID, rootID: view.rootID, runID, frameID: frame.id, decision, policies, original: original[index], prepared, pointerRevision: revisions[index] } };
          const job = admission.find(item => admissionKey(item) === prepared.key && dispatchPayloadMatches(payload, item.payload_json));
          if (!job) return;
          const conclusion = await readValidationConclusionAsync(store, job.report_json);
          if (!conclusion.valid) {
            block(work, view, conclusion.issue?.code === "FORBIDDEN" ? "FORBIDDEN" : "INVALID_DECISION", "Action input validation or authorization failed", json({ frameID: frame.id, key: rewritten[index]!.key }),
              [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at]), write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND payload_json=? AND report_json=?", ["accepted", job.id as string, "accepted", job.payload_json as string, job.report_json as string])], deadline);
            return;
          }
          matched.push(job);
        }
        const writes: SQLWrite[] = [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", roundID, "pending", at]),
          write("UPDATE dispatch_preparations SET state=? WHERE frame_id=? AND round_id=? AND state=? AND generation=?", ["ready", frame.id as string, roundID, "validation", preparation.generation as number])];
        for (const job of matched) writes.push(write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND payload_json=? AND report_json=? AND assignment_id=? AND assignment_epoch=?", ["accepted", job.id as string, "accepted", job.payload_json as string, job.report_json as string, job.assignment_id as string, job.assignment_epoch as number]));
        commit(randomUUID(), "run.dispatch_validated", runID, { frameID: frame.id, roundID, generation: preparation.generation }, writes, null, [], deadline);
        return;
      }
      if (preparation.state === "ready") {
        const resolved = readPreparationResolved(store, preparation);
        const revisions = decode<(number | null)[]>(preparation.pointer_revisions_json);
        const jobs = store.query("SELECT * FROM validation_jobs WHERE round_id=? AND state<>?", [roundID, "superseded"]);
        const admission = jobs.filter(item => decode<{ phase: string }>(item.payload_json).phase === "admission");
        const pointerChanged = resolved.some((request, index) => {
          if (revisions[index] === null) return false;
          const pointer = store.query("SELECT version,revision FROM version_pointers WHERE kind=? AND id=?", [request.kind, request.target.id])[0];
          return !pointer || pointer.version !== request.target.version || pointer.revision !== revisions[index];
        });
        if (pointerChanged) {
          const writes: SQLWrite[] = [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", roundID, "pending", at]),
            write("UPDATE dispatch_preparations SET state=?,resolved_json=NULL,pointer_revisions_json=NULL WHERE frame_id=? AND round_id=? AND state=? AND generation=?", ["resolve", frame.id as string, roundID, "ready", preparation.generation as number])];
          let validBefore = deadline;
          if (work.stage === "finalize") {
            const finalization = store.query("SELECT * FROM finalize_jobs WHERE id=? AND run_id=?", [frame.id as string, runID])[0];
            if (finalization) {
              let grant: LeaseGrant | null = null;
              if (finalization.state === "accepted") grant = await finalizationGrant(finalization, frame, view, row.principal_id as string);
              else if (finalization.state === "assigned" && finalization.assignment_json) {
                const assignment = await readFrameAssignment(store, finalization);
                try { leases.assert(assignment.lease); grant = assignment.lease; } catch { /* The old owner no longer qualifies. */ }
              }
              if (grant) {
                writes.push(...leases.guard(grant), write("UPDATE finalize_jobs SET state=? WHERE id=? AND state=? AND assignment_epoch=? AND lease_epoch=?", [finalization.state as string, frame.id as string, finalization.state as string, finalization.assignment_epoch as number, grant.epoch]), releaseGrant(grant));
                validBefore = Math.min(validBefore, grant.expiresAt);
              }
            }
            writes.push(stage(work.id as string, "finalize", "dispatch_prepare"));
          }
          for (const job of admission) writes.push(write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND assignment_epoch=?", ["superseded", job.id as string, job.state as string, job.assignment_epoch as number]));
          commit(randomUUID(), "run.dispatch_reprepare", runID, { frameID: frame.id, roundID, generation: preparation.generation }, writes, null, [], validBefore);
          return;
        }
        if (work.stage === "dispatch_prepare") {
          const existingFinalization = store.query("SELECT * FROM finalize_jobs WHERE id=? AND run_id=?", [frame.id as string, runID])[0];
          const writes: SQLWrite[] = [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", roundID, "pending", at]),
            write("UPDATE dispatch_preparations SET state=? WHERE frame_id=? AND round_id=? AND state=? AND generation=?", ["ready", frame.id as string, roundID, "ready", preparation.generation as number]),
            ...(existingFinalization ? [write("UPDATE finalize_jobs SET state=?,grant_json=NULL WHERE id=? AND run_id=? AND state=? AND assignment_epoch=?", ["pending", frame.id as string, runID, existingFinalization.state as string, existingFinalization.assignment_epoch as number])] : [insertFinalization(frame, view, row.principal_id as string)]),
            stage(work.id as string, "dispatch_prepare", "finalize")];
          for (const [index, request] of resolved.entries()) if (revisions[index] !== null) writes.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [revisions[index]!, request.kind, request.target.id, request.target.version, revisions[index]!]));
          commit(randomUUID(), "run.dispatch_finalize_ready", runID, { frameID: frame.id, roundID, generation: preparation.generation }, writes, null, [], deadline);
          return;
        }
        const finalization = store.query("SELECT * FROM finalize_jobs WHERE id=? AND run_id=? AND state=?", [frame.id as string, runID, "accepted"])[0];
        const grant = await finalizationGrant(finalization, frame, view, row.principal_id as string);
        if (!finalization || !grant) return;
        if (!workerAllowed(grant)) return;
        try { options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent); }
        catch {
          block(work, view, "FORBIDDEN", "Run Agent is no longer authorized", json({ frameID: frame.id }), [...leases.guard(grant), finalizeGuard(finalization, grant), releaseGrant(grant), write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at])], Math.min(deadline, grant.expiresAt));
          return;
        }
        // The root record is the shared budget authority for the whole tree.
        const rootRow = store.query("SELECT principal_id,snapshot_json FROM runs WHERE id=?", [view.rootID])[0];
        if (!rootRow?.snapshot_json) return;
        const root = readRunSnapshot(store, rootRow.snapshot_json);
        let depth = 0, ancestor = view;
        const seen = new Set<string>();
        while (ancestor.id !== view.rootID) {
          if (seen.has(ancestor.id) || !ancestor.parentActionID) return;
          seen.add(ancestor.id); depth++;
          const parent = store.query("SELECT r.snapshot_json,r.principal_id FROM actions a JOIN runs r ON r.id=a.run_id WHERE a.id=?", [ancestor.parentActionID])[0];
          if (!parent || parent.principal_id !== row.principal_id) return;
          ancestor = readRunSnapshot(store, parent.snapshot_json);
        }
        if (root.id !== root.rootID || root.parentActionID !== null || rootRow.principal_id !== row.principal_id
            || canonicalJson(root.limits) !== canonicalJson(view.limits)) return;
        if (root.rootActionCount + resolved.length > root.limits.maxActions || (resolved.some(action => action.kind === "agent") && depth >= root.limits.maxDepth)) {
          block(work, view, "LIMIT_EXCEEDED", "Root action or depth limit reached", json({ frameID: frame.id }), [...leases.guard(grant), finalizeGuard(finalization, grant), releaseGrant(grant)], Math.min(deadline, grant.expiresAt));
          return;
        }
        const matched: Row[] = [];
        if (admission.length !== resolved.length) return;
        for (const [index, request] of resolved.entries()) {
          try { options.authorizePrincipal(row.principal_id as string, "client.submit", request.kind, request.target); }
          catch {
            block(work, view, "FORBIDDEN", "Action target is no longer authorized", json({ frameID: frame.id, key: request.key }), [...leases.guard(grant), finalizeGuard(finalization, grant), releaseGrant(grant), write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["expired", roundID, "pending", at])], Math.min(deadline, grant.expiresAt));
            return;
          }
          const expected = { kind: request.kind, ref: request.target, phase: "admission", value: request.input, config: request.config, principalID: row.principal_id,
            operation: "client.submit", context: { domainID: store.domainID, rootID: view.rootID, runID, frameID: frame.id, decision, policies, original: original[index], prepared: request, pointerRevision: revisions[index] } };
          const job = admission.find(item => item.state === "accepted" && admissionKey(item) === request.key && dispatchPayloadMatches(expected, item.payload_json));
          if (!job || !(await readValidationConclusionAsync(store, job.report_json)).valid) return;
          matched.push(job);
        }
        const existingGuards: SQLWrite[] = [], reusedActionWitnesses: Json[] = [];
        const verifyReuse = async (previous: Row, key: string): Promise<ActionRequest> => {
          const identity = await readActionReuseIdentity(store, previous);
          if (previous.snapshot_projection_id !== null) reusedActionWitnesses.push(json({ actionID: previous.id, runID, actionKey: key,
            revision: previous.revision, snapshotProjectionID: previous.snapshot_projection_id,
            original: await canonicalValueWitness(identity.original), prepared: await canonicalValueWitness(identity.request) }));
          return identity.original;
        };
        for (const action of decision.actions) {
          if (original.some(item => item.key === action.key)) continue;
          const previous = store.query("SELECT * FROM actions WHERE run_id=? AND action_key=?", [runID, action.key])[0];
          if (!previous || !previous.snapshot_json || !sameJsonChunks(await verifyReuse(previous, action.key), action)) return;
          existingGuards.push(write("UPDATE actions SET revision=revision+0 WHERE id=? AND run_id=? AND action_key=? AND revision=? AND request_json=? AND snapshot_json=? AND snapshot_projection_id IS ?", [previous.id as string, runID, action.key, previous.revision as number, previous.request_json as string, previous.snapshot_json as string, previous.snapshot_projection_id as string | null]));
        }
        for (const key of decision.wait.all) {
          if (decision.actions.some(action => action.key === key)) continue;
          const previous = store.query("SELECT * FROM actions WHERE run_id=? AND action_key=?", [runID, key])[0];
          if (!previous || !previous.snapshot_json) return;
          await verifyReuse(previous, key);
          existingGuards.push(write("UPDATE actions SET revision=revision+0 WHERE id=? AND run_id=? AND action_key=? AND revision=? AND request_json=? AND snapshot_json=? AND snapshot_projection_id IS ?", [previous.id as string, runID, key, previous.revision as number, previous.request_json as string, previous.snapshot_json as string, previous.snapshot_projection_id as string | null]));
        }
        const timeout = decision.wait.timeoutMs === undefined ? view.limits.defaultWaitTimeoutMs : decision.wait.timeoutMs;
        const waitDeadline = timeout === null ? null : at + timeout;
        if (waitDeadline !== null && !Number.isSafeInteger(waitDeadline)) { block(work, view, "LIMIT_EXCEEDED", "Wait deadline is out of range", json({ frameID: frame.id }), [...leases.guard(grant), finalizeGuard(finalization, grant), releaseGrant(grant)], Math.min(deadline, grant.expiresAt)); return; }
        const waitID = randomUUID();
        const fixedFacts = await fixedFrameFacts(frame), frameFactIDs = [...fixedFacts.factIDs].sort();
        const existingFacts: Fact[] = [], factSnapshot: { actionID: string; factID: string | null }[] = [];
        let missingFact = false;
        for (const key of decision.wait.all) {
          if (original.some(item => item.key === key)) { missingFact = true; continue; }
          const existing = store.query("SELECT id FROM actions WHERE run_id=? AND action_key=?", [runID, key])[0];
          if (!existing) return;
          const fact = store.query("SELECT id,action_id,outcome_json,outcome_projection_id FROM facts WHERE action_id=? AND run_id=?", [existing.id as string, runID])[0];
          factSnapshot.push({ actionID: existing.id as string, factID: fact ? fact.id as string : null });
          if (fact) existingFacts.push({ id: fact.id as string, actionID: existing.id as string, actionKey: key, outcome: await readFactOutcome(store, fact) });
          else missingFact = true;
        }
        const initialWake: Extract<Wake, { kind: "results" }> | null = missingFact ? null : { kind: "results", waitID, facts: existingFacts };
        const initialWakeWitnesses: Json[] | null = initialWake && initialWakeNeedsReference(initialWake) ? [] : null;
        if (initialWakeWitnesses && initialWake) for (const fact of initialWake.facts) initialWakeWitnesses.push(await waitFactWitness(fact));
        if (!workerAllowed(grant)) return;
        try {
          leases.assert(grant);
          options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent);
          for (const request of resolved) options.authorizePrincipal(row.principal_id as string, "client.submit", request.kind, request.target);
        } catch { return; }
        const stagingGuards = () => [...leases.guard(grant),
          write("UPDATE dispatch_preparations SET state='ready' WHERE frame_id=? AND round_id=? AND generation=? AND state='ready'", [frame.id as string, roundID, preparation.generation as number]),
          write("UPDATE prepare_rounds SET state='pending' WHERE id=? AND state='pending' AND deadline_at>?", [roundID, now()]),
          write("UPDATE finalize_jobs SET state='accepted' WHERE id=? AND state='accepted' AND assignment_id=?", [frame.id as string, finalization.assignment_id as string])];
        let actionRequestValues: ActionRequestValue[];
        try {
          actionRequestValues = await stageActionRequestValues(store, preparation, finalization.assignment_id as string, resolved, options.valueQuota ?? Number.MAX_SAFE_INTEGER,
            () => { leases.assert(grant); if (!workerAllowed(grant)) throw harnessError("EXECUTION_FENCED", "Request staging worker is unavailable"); }, stagingGuards);
        } catch (error) {
          if (error instanceof HarnessError && error.info.code === "LIMIT_EXCEEDED" && !store.requiresRecovery) {
            block(work, view, "LIMIT_EXCEEDED", "Action request staging quota is full", json({ frameID: frame.id }), [...leases.guard(grant), releaseGrant(grant)], Math.min(deadline, grant.expiresAt)); return;
          }
          throw error;
        }
        const snapshot = core.getSnapshot();
        if (!snapshot?.executionToken) return;
        // Staging committed independently. Only a fresh post-staging witness
        // can prove a rejected Core handoff did not publish these values.
        if (actionRequestValues.length) workWitness = store.armRecoveryWitness();
        await transition(runID, core, () => core.suspend(snapshot.executionToken!, async proposal => {
          const initialWakeStorage = initialWakeWitnesses ? makeInitialWaitWake(runID, frame.id as string, waitID, proposal.operationID, initialWakeWitnesses) : initialWake;
          const initialWaitHistory = initialWake ? referencedWaitResolutionEvents(store, runID, waitID, initialWake.facts, options.maxResponseBytes, undefined, proposal.operationID) : { events: [], writes: [] };
          const updated: RunView = { ...view, lifecycle: "suspended", continuation: decision.continuation, wait: { id: waitID, actionKeys: decision.wait.all, deadlineAt: waitDeadline, winner: initialWake ? "results" : null },
            rootActionCount: root.rootActionCount + resolved.length, revision: view.revision + 1, updatedAt: at };
          const writes: SQLWrite[] = [...leases.guard(grant), finalizeGuard(finalization, grant), releaseGrant(grant), ...existingGuards,
            write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["accepted", roundID, "pending", at]),
            write("UPDATE dispatch_preparations SET state=? WHERE frame_id=? AND round_id=? AND state=? AND generation=? AND resolved_json=? AND pointer_revisions_json=?", ["ready", frame.id as string, roundID, "ready", preparation.generation as number, preparation.resolved_json as string, preparation.pointer_revisions_json as string]),
            runWrite(updated, view, json(proposal.after), json(proposal.before), undefined, continuationSource(runID, frame.id as string, proposal.operationID, decision.continuation)),
            write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=? AND input_json=?", ["committed", frame.id as string, runID, "open", frame.input_json as string]),
            ...initialWaitHistory.writes,
            write("INSERT INTO waits (id,run_id,frame_id,action_keys_json,deadline_at,winner,wake_json) VALUES (?,?,?,?,?,?,?)", [waitID, runID, frame.id as string, { json: json(decision.wait.all) }, waitDeadline, initialWake ? "results" : null, initialWakeStorage ? { json: json(initialWakeStorage) } : null]),
            write("UPDATE work_items SET stage=?,result_json=? WHERE id=? AND stage=?", [initialWake ? "wake" : "waiting", null, work.id as string, "finalize"])];
          if (root.id !== runID) writes.push(runWrite({ ...root, rootActionCount: root.rootActionCount + resolved.length, revision: root.revision + 1, updatedAt: at }, root));
          const actions: ActionView[] = [], actionWorkIDs: string[] = [], children: RunView[] = [];
          for (const [index, request] of resolved.entries()) {
            if (revisions[index] !== null) writes.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [revisions[index]!, request.kind, request.target.id, request.target.version, revisions[index]!]));
            const job = matched[index]!;
            writes.push(write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND payload_json=? AND report_json=? AND assignment_id=? AND assignment_epoch=?", ["accepted", job.id as string, "accepted", job.payload_json as string, job.report_json as string, job.assignment_id as string, job.assignment_epoch as number]));
            const actionID = randomUUID();
            const childID = request.kind === "agent" ? randomUUID() : null;
            const action: ActionView = { id: actionID, runID, revision: 0, original: original[index]!, request, status: "accepted", latestAttempt: null, executionCount: 0,
              checkpoint: { revision: 0, value: null }, outcome: null, outcomeObservationID: null, cancelRequested: false, cancellation: null, blocked: null,
              recheckRequested: false, remoteRunID: childID, latestObservation: null, createdAt: at, updatedAt: at, settledAt: null };
            const actionWorkID = randomUUID();
            actions.push(action); actionWorkIDs.push(actionWorkID);
            writes.push(write("INSERT INTO actions (id,run_id,action_key,revision,epoch,request_json,outcome_json,snapshot_json) VALUES (?,?,?,?,?,?,?,?)", [actionID, runID, request.key, 0, 0, { json: encodeFixedActionRequest(request, actionRequestValues, policySource) }, null, { json: encodeActionSnapshot(action) }]));
            if (childID) {
              const child: RunView = { id: childID, rootID: view.rootID, parentActionID: actionID, revision: 0, agent: request.target,
                input: request.input, config: request.config, lifecycle: null, continuation: { entry: "start", state: null }, wait: null,
                outcome: null, cancelRequested: false, cancellation: null, blocked: null, limits: view.limits,
                rootActionCount: root.rootActionCount + resolved.length, frameCount: 0, createdAt: at, updatedAt: at, completedAt: null };
              children.push(child);
              writes.push(write("INSERT INTO runs (id,principal_id,revision,epoch,frame_count,snapshot_json) VALUES (?,?,?,?,?,?)", [childID, row.principal_id as string, 0, 0, 0, { json: encodeRunInput(store, child) }]),
                write("INSERT INTO remote_run_links (action_id,child_run_id) VALUES (?,?)", [actionID, childID]),
                write("INSERT INTO work_items (id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES (?,?,?,?,?,?,?,?)", [actionWorkID, "run", childID, "start", 0, at, { json: json({ runID: childID }) }, null]));
            } else writes.push(write("INSERT INTO work_items (id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES (?,?,?,?,?,?,?,?)", [actionWorkID, "action", actionID, "accepted", 0, at, { json: json({ runID, actionID }) }, null]));
          }
          writes.push(...requestValuePublicationWrites(actionRequestValues, finalization.assignment_id as string));
          try {
            const admissionEvidence = matched.map(job => ({ id: job.id, assignmentID: job.assignment_id, assignmentEpoch: job.assignment_epoch, payload: decode(job.payload_json), conclusion: readValidationConclusion(store, job.report_json) }));
            commit(proposal.operationID, "core.suspend", runID, dispatchRequest(store, runID, { proposal, frameID: frame.id, decision, grant, finalizeAssignment: decode(finalization.assignment_json), roundID, deadline, waitID, waitDeadline, initialWake: initialWakeStorage, ...(actionRequestValues.length ? { actionRequestValues } : {}), priorRootActionCount: root.rootActionCount, frameFactIDs, actions, actionWorkIDs, children, rootAfter: root.id === runID ? updated : { ...root, rootActionCount: root.rootActionCount + resolved.length, revision: root.revision + 1, updatedAt: at }, resolved, revisions, generation: preparation.generation, admissionEvidence,
              ...(reusedActionWitnesses.length ? { reusedActionWitnesses } : {}) }), writes, proposal,
              [dispatchDecisionEvent(runID, decision, frame.id as string, proposal.operationID), { type: "run.wait_created", subjectID: runID, data: json({ waitID, frameID: frame.id, deadlineAt: waitDeadline }) },
                { type: "wait.created", subjectID: runID, data: json({ waitID, frameID: frame.id, actionKeys: decision.wait.all, deadlineAt: waitDeadline }) },
                ...actions.map(action => actionAcceptanceEvent(action, frame.id as string)),
                ...children.map(child => childAcceptanceEvent(child, row.principal_id as string)),
                ...initialWaitHistory.events], Math.min(deadline, grant.expiresAt), undefined, fixedFacts, factSnapshot, actionRequestValues.map(item => item.source.value));
            return "accepted";
          } catch (error) { return isUncertain(error) ? "unknown" : "rejected"; }
        }));
        if (!quarantined.has(runID)) cores.delete(runID);
        return;
      }
      return;
    }
    let round: Row | undefined, job: Row | undefined;
    if (decision.outcome.kind === "succeeded") {
      round = store.query("SELECT * FROM prepare_rounds WHERE owner_kind=? AND owner_id=? AND state=?", ["frame", frame.id as string, "pending"])[0];
      if (!round) return;
      if (now() >= (round.deadline_at as number)) { block(work, view, "VERSION_UNAVAILABLE", "Output validation expired", json({ phase: "output", roundID: round.id, deadlineAt: round.deadline_at, retryAfterMs: options.retryAfterMs }), [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=?", ["expired", round.id as string, "pending"])]); return; }
      job = store.query("SELECT * FROM validation_jobs WHERE round_id=? AND state=?", [round.id as string, "accepted"])[0];
      if (!job) return;
      const payload = agentOutputValidation(store, view, frame.id as string, row.principal_id as string, decision, decode<Json>(job.payload_json));
      if (canonicalJson(payload) !== job.payload_json) return;
      const conclusion = await readValidationConclusionAsync(store, job.report_json);
      if (!conclusion.valid) { block(work, view, "OUTPUT_VALIDATION_FAILED", "Agent output validation failed", json({ frameID: frame.id, issue: conclusion.issue }), [write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=?", ["expired", round.id as string, "pending"])]); return; }
      if (await frameExceedsBytes(decision.outcome.value, options.maxValueBytes)) { block(work, view, "LIMIT_EXCEEDED", "Output exceeds size limit", json({ frameID: frame.id })); return; }
    }
    if (work.stage !== "finalize") {
      const at = now();
      const existingFinalization = store.query("SELECT * FROM finalize_jobs WHERE id=? AND run_id=?", [frame.id as string, runID])[0];
      const writes: SQLWrite[] = [
        write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=? AND input_json=?", ["open", frame.id as string, runID, "open", frame.input_json as string]),
        ...(existingFinalization ? [write("UPDATE finalize_jobs SET state=?,grant_json=NULL WHERE id=? AND run_id=? AND state=? AND assignment_epoch=?", ["pending", frame.id as string, runID, existingFinalization.state as string, existingFinalization.assignment_epoch as number])] : [insertFinalization(frame, view, row.principal_id as string)]),
        stage(work.id as string, work.stage as string, "finalize"),
      ];
      if (round && job) writes.push(write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", round.id as string, "pending", at]),
        write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND payload_json=? AND report_json=? AND assignment_id=? AND assignment_epoch=?", ["accepted", job.id as string, "accepted", job.payload_json as string, job.report_json as string, job.assignment_id as string, job.assignment_epoch as number]));
      commit(randomUUID(), "run.finalize_ready", runID, { frameID: frame.id, decision: decode(candidate.decision_json) }, writes, null, [], round?.deadline_at as number | undefined);
      return;
    }
    const finalization = store.query("SELECT * FROM finalize_jobs WHERE id=? AND run_id=? AND state=?", [frame.id as string, runID, "accepted"])[0];
    const grant = await finalizationGrant(finalization, frame, view, row.principal_id as string);
    if (!finalization || !grant) return;
    if (!workerAllowed(grant)) return;
    options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent);
    const snapshot = core.getSnapshot();
    if (!snapshot || !snapshot.executionToken) return;
    const at = now();
    await transition(runID, core, () => core.complete({ executionToken: snapshot.executionToken, outcome: decision.outcome.kind }, async proposal => {
      const updated: RunView = { ...view, lifecycle: "completed", outcome: decision.outcome, revision: view.revision + 1, updatedAt: at, completedAt: at,
        ...(decision.outcome.kind === "cancelled" ? { cancelRequested: true, cancellation: { source: "agent", reason: decision.outcome.reason, requestedAt: at } } : {}) };
      const writes = [...leases.guard(grant), finalizeGuard(finalization, grant), releaseGrant(grant),
        runWrite(updated, view, json(proposal.after), json(proposal.before), { frameID: frame.id as string, operationID: proposal.operationID }), write("UPDATE frames SET state=? WHERE id=? AND run_id=? AND state=? AND input_json=?", ["committed", frame.id as string, runID, "open", frame.input_json as string]),
        stage(work.id as string, work.stage as string, "done")];
      const cancellation = { source: decision.outcome.kind === "cancelled" ? "agent" : "completion", reason: decision.outcome.kind === "cancelled" ? decision.outcome.reason : null, requestedAt: at };
      const barrierID = randomUUID();
      writes.push(write("INSERT INTO cancellation_barriers(id,scope,run_id,action_id,cancellation_json) VALUES(?,?,?,?,?)", [barrierID, "remaining_actions", runID, null, { json: json(cancellation) }]));
      const cleanupRequired = store.query("SELECT id FROM actions WHERE run_id=? AND outcome_json IS NULL LIMIT 1", [runID]).length > 0;
      if (cleanupRequired) writes.push(write("INSERT INTO work_items(id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES(?,?,?,?,?,?,?,?)", [JSON.stringify(["cancel.cleanup", runID]), "cancel_cleanup", runID, "pending", 0, at, { json: json({ runID, barrierID }) }, null]));
      // Candidate identity and content are immutable and retained by schema triggers.
      if (round && job) writes.push(write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["accepted", round.id as string, "pending", at]),
        write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND payload_json=? AND report_json=? AND assignment_id=? AND assignment_epoch=?", ["accepted", job.id as string, "accepted", job.payload_json as string, job.report_json as string, job.assignment_id as string, job.assignment_epoch as number]));
      try {
        const remote = await prepareRemoteCompletion(store, view, updated, proposal.operationID, at, options.maxResponseBytes);
        if (!workerAllowed(grant)) return "rejected";
        try { leases.assert(grant); options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent); }
        catch { return "rejected"; }
        writes.push(...(remote?.writes ?? []));
        const fixedFacts = await fixedFrameFacts(frame);
        if (!workerAllowed(grant)) return "rejected";
        leases.assert(grant); options.authorizePrincipal(row.principal_id as string, "client.submit", "agent", view.agent);
        commit(proposal.operationID, "core.complete", runID, completionRequest(store, runID, { remote: remote?.evidence ?? null, proposal, frameID: frame.id, decision, grant, finalizeAssignment: decode(finalization.assignment_json), completionBarrierID: barrierID, cleanupRequired, frameFactIDs: [...fixedFacts.factIDs].sort(), validation: job ? { payload: decode(job.payload_json), conclusion: readValidationConclusion(store, job.report_json), roundID: round!.id, jobID: job.id } : null }), writes, proposal,
          [...completionEvents(updated, decision, frame.id as string, proposal.operationID), ...(remote?.events ?? [])], Math.min(grant.expiresAt, round?.deadline_at as number ?? Number.MAX_SAFE_INTEGER), undefined, fixedFacts, remote?.factSnapshot);
        return "accepted";
      } catch (error) { return isUncertain(error) ? "unknown" : "rejected"; }
    }));
    if (!quarantined.has(runID)) cores.delete(runID);
  }
  async function cleanup(work: Row) {
    const runID = work.subject_id as string;
    const row = store.query("SELECT * FROM actions WHERE run_id=? AND outcome_json IS NULL ORDER BY creation_sequence LIMIT 1", [runID])[0];
    if (!row) {
      store.commit({ operationID: randomUUID(), kind: "cancel.cleanup_done", subjectID: runID, request: json({ runID }), writes: [stage(work.id as string, "pending", "done")], evidence: null, result: null });
      return;
    }
    const action = await readActionSnapshot(store, row), cancellation = store.effectiveCancellation({ kind: "action", id: action.id });
    if (!cancellation) return;
    const at = now(), outcome: Outcome = { kind: "cancelled", reason: cancellation.reason };
    const knownEffect = action.executionCount > 0 ? resolvedEffectGuard(store, action) : null;
    const unknownEffect = action.executionCount > 0 && !knownEffect;
    const blocked = action.blocked ?? (unknownEffect ? { code: "RECONCILIATION_REQUIRED", message: "External effect remains unknown after cancellation", details: json({ actionID: action.id, attemptID: action.latestAttempt?.id ?? null, source: "cancellation" }) } : null);
    const updated: ActionView = { ...action, revision: action.revision + 1, updatedAt: at, status: "settled", outcome, settledAt: at, cancelRequested: true, cancellation, blocked };
    const actionWork = store.query("SELECT id,stage FROM work_items WHERE kind=? AND subject_id=?", ["action", action.id])[0];
    const writes: SQLWrite[] = [write("UPDATE actions SET revision=?,snapshot_json=?,outcome_json=? WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS NULL AND outcome_projection_id IS NULL", [updated.revision, { json: encodeActionSnapshot(updated) }, { json: json(outcome) }, action.id, action.revision, row.snapshot_json as string])];
    if (knownEffect) writes.unshift(knownEffect);
    // An Agent Action and its child were admitted together. Propagate along that
    // durable mapping in the same transaction that settles the parent Action.
    // The ancestry barrier already fences the entire subtree before this batch.
    if (action.request.kind === "agent") {
      const link = store.query("SELECT child_run_id FROM remote_run_links WHERE action_id=?", [action.id])[0];
      if (!link || link.child_run_id !== action.remoteRunID) throw new Error("Invalid child Run mapping during cancellation");
      const child = store.query("SELECT snapshot_json,revision FROM runs WHERE id=?", [action.remoteRunID!])[0];
      if (!child) throw new Error("Missing child Run during cancellation");
      const childView = readRunSnapshot(store, child.snapshot_json);
      if (childView.parentActionID !== action.id) throw new Error("Invalid child Run parent during cancellation");
      // A terminal child retains its own result and its previously registered
      // cleanup. An active child receives durable control and cleanup work.
      if (!childView.outcome) {
        const childWork = store.query("SELECT id,stage FROM work_items WHERE kind=? AND subject_id=?", ["run", childView.id])[0];
        if (!childWork || typeof childWork.id !== "string" || typeof childWork.stage !== "string" || childWork.stage === "done") throw new Error("Missing child Run control work");
        writes.push(write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [childView.id, child.revision as number, child.snapshot_json as string]));
        if (childWork.stage !== "cancel" && childWork.stage !== "cancel_control") writes.push(stage(childWork.id, childWork.stage, "cancel"));
        const cleanupID = JSON.stringify(["cancel.cleanup", childView.id]);
        const existing = store.query("SELECT kind,subject_id FROM work_items WHERE id=?", [cleanupID])[0];
        if (existing && (existing.kind !== "cancel_cleanup" || existing.subject_id !== childView.id)) throw new Error("Invalid child cleanup work");
        if (!existing) writes.push(write("INSERT INTO work_items(id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES(?,?,?,?,?,?,?,?)", [cleanupID, "cancel_cleanup", childView.id, "pending", 0, at, { json: json({ runID: childView.id, sourceActionID: action.id }) }, null]));
      }
    }
    if (actionWork) {
      // Cancellation settles the business result but must not discard an
      // explicitly authorized recheck or steal its in-flight read-only round.
      const recheck = actionWork.stage === "recheck" && action.recheckRequested
        || (actionWork.stage === "running" || actionWork.stage === "report_pending") && action.latestAttempt?.mode === "recheck-only";
      writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", [recheck ? actionWork.stage as string : "done", actionWork.id as string, "action", action.id, actionWork.stage as string]));
    }
    const fact = store.query("SELECT id FROM facts WHERE action_id=? AND run_id=?", [action.id, runID])[0];
    if (!fact) writes.push(write("INSERT INTO facts(id,run_id,action_id,outcome_json) VALUES(?,?,?,?)", [randomUUID(), runID, action.id, { json: json(outcome) }]));
    store.commit({ operationID: randomUUID(), kind: "cancel.action_cleanup", subjectID: action.id, request: json({ runID, actionID: action.id, cancellation }), writes, evidence: json(outcome), result: null,
      events: [{ type: "action.cancelled", subjectID: action.id, data: json({ cancellation, outcome }) },
        { type: "action.settled", subjectID: action.id, data: json({ outcome, sourceObservationID: null, settledAt: at }) },
        ...(blocked && !action.blocked ? [{ type: "action.blocked", subjectID: action.id, data: json({ blocked }) }] : [])] });
  }
  function recoverControlStorage(): boolean {
    const pending = store.unresolvedCommit, sealed = sealedControl;
    if (!store.requiresRecovery || !pending || !sealed || !sealed.released
        || pending.operationID !== sealed.batch.operationID || canonicalJson(pending) !== canonicalJson(sealed.batch)
        || transitionsInFlight.size || unknownCores.size || pausedControlOperation === pending.operationID) return false;
    try {
      const recovery = store.recoverAuthority();
      if (recovery.oldEpoch !== sealed.epoch) return false;
      const proof = store.getCommit(pending.operationID);
      const record = store.query("SELECT request_json FROM history WHERE operation_id=?", [pending.operationID])[0];
      const row = store.query("SELECT * FROM runs WHERE id=?", [pending.subjectID])[0];
      const expectedWork = decode<Row>(sealed.work);
      const work = store.query("SELECT * FROM work_items WHERE id=? AND kind='run' AND subject_id=?", [expectedWork.id as string, pending.subjectID])[0];
      const request = pending.request as { cancellation?: RunView["cancellation"] };
      const receiptMatches = proof ? !!record && proof.operationID === pending.operationID && proof.kind === pending.kind && proof.subjectID === pending.subjectID
        && record.request_json === canonicalJson(pending.request) && canonicalJson(proof.evidence) === canonicalJson(pending.evidence)
        && canonicalJson(proof.result) === canonicalJson(pending.result) : !record;
      if (!receiptMatches || !row || canonicalJson(row) !== sealed.run
          || !work || canonicalJson(work) !== (proof ? sealed.work : sealed.beforeWork) || cores.get(pending.subjectID) !== sealed.core
          || canonicalJson(sealed.core?.getSnapshot() ?? null) !== sealed.snapshot
          || !request.cancellation || canonicalJson(store.effectiveCancellation({ kind: "run", id: pending.subjectID })) !== canonicalJson(request.cancellation)) {
        // Absence is usable only with the sealed pre-state and a released Core
        // slot. A hidden/mismatched receipt or changed state remains isolated.
        pausedControlOperation = pending.operationID;
        return false;
      }
      store.finishRecovery();
      sealedControl = undefined;
      pausedControlOperation = undefined;
      if (work.stage !== "cancel") {
        // Only the work reservation changes here. The original Core command
        // was never sent (arm), or definitely rejected (intent); its stable
        // handle and original frame remain in place for the next cancellation.
        commitControlStep({ operationID: `cancel.control_recover:${pending.operationID}`, kind: proof ? "cancel.control_recovered_committed" : "cancel.control_recovered_uncommitted", subjectID: pending.subjectID,
          request: json({ originalOperationID: pending.operationID, cancellation: request.cancellation }),
          writes: [write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND core_json IS ?", [pending.subjectID, row.revision as number, row.snapshot_json as string, row.core_json as string | null]),
            write("UPDATE work_items SET stage=?,result_json=NULL WHERE id=? AND kind='run' AND subject_id=? AND stage=? AND result_json IS ?", ["cancel", work.id as string, pending.subjectID, work.stage as string, work.result_json as string | null])],
          evidence: json({ originalOperationID: pending.operationID }), result: null }, row, { ...work, stage: "cancel", result_json: null }, sealed.core, true);
      }
      return true;
    } catch { return false; }
  }
  /** Reopen a frozen non-Core commit without claiming that a missing receipt is
   * a rejection. Callers keep their original operation/request identity; normal
   * scanners reconstruct work only from the newly authoritative database. */
  async function recoverNonCoreStorage(): Promise<boolean> {
    const pending = store.unresolvedCommit;
    if (!store.requiresRecovery || !pending || pending.kind.startsWith("core.") || pending.kind.startsWith("cancel.control_")
        || transitionsInFlight.size || unknownCores.size || pausedNonCoreOperation === pending.operationID) return false;
    try {
      const oldEpoch = store.coordinatorEpoch;
      const recovery = store.recoverAuthority();
      if (recovery.newEpoch !== store.coordinatorEpoch || recovery.oldEpoch > oldEpoch) return false;
      const proof = store.getCommit(pending.operationID);
      // On the continuous authoritative database, every committed batch has
      // its immutable receipt. The old connection is closed and fenced, so an
      // absent receipt leaves no original transaction that could still commit.
      // Unfreeze only: do not resolve a Core proposal, record a rejection or
      // replay the batch. Callers and scanners retain their normal identities.
      if (!proof) {
        store.finishRecovery();
        pausedNonCoreOperation = undefined;
        return true;
      }
      if (proof.operationID !== pending.operationID || proof.kind !== pending.kind || proof.subjectID !== pending.subjectID
          || canonicalJson(proof.evidence) !== canonicalJson(pending.evidence) || canonicalJson(proof.result) !== canonicalJson(pending.result)
          || !store.matchesCommitEvidence(pending) || !await verifyDiagnosticTransferForEvidence(store, pending)) { pausedNonCoreOperation = pending.operationID; return false; }
      store.finishRecovery();
      pausedNonCoreOperation = undefined;
      return true;
    } catch { return false; }
  }
  async function scan() {
    scheduled = undefined; if (closed || scanning) return; scanning = true;
    try {
      if (store.requiresRecovery && !recoverControlStorage() && !await recoverNonCoreStorage()) return;
      await retireDriverValues(store);
      await retireActionRequestValues(store);
      await retireSubmitInputs(store);
      await retireCheckpointValues(store);
      await retireNodePolicies(store);
      await retireValidationValues(store);
      for (const unresolved of store.query("SELECT id,subject_id,result_json FROM work_items WHERE kind=? AND stage=?", ["run", "cancel_control"])) {
        const runID = unresolved.subject_id as string;
        if (unresolved.result_json !== null) {
          if (!recoverColdControlIntent(unresolved)) quarantined.add(runID);
        }
        else {
          // The target Core commit is only called after its complete intent was
          // confirmed. No intent therefore proves this armed proposal was unsent.
          store.commit({ operationID: randomUUID(), kind: "cancel.control_unarm", subjectID: runID, request: json({ runID }),
            writes: [write("UPDATE work_items SET stage=? WHERE id=? AND stage=? AND result_json IS NULL", ["cancel", unresolved.id as string, "cancel_control"])], evidence: null, result: null });
        }
      }
      let cursor: [number, string] | undefined;
      for (;;) {
        store.assertAuthority();
        const rows = store.query(`SELECT * FROM work_items WHERE kind='run' AND stage IN ('start','driver','validation','complete','finalize','dispatch_prepare','waiting','wake','cancel')${cursor ? " AND (available_at,id)>(?,?)" : ""} ORDER BY available_at,id LIMIT 16`, [...(cursor ?? [])]);
        for (const row of rows) {
          if (closed) break;
          workWitness = undefined;
          try {
            if (["start", "wake", "finalize", "cancel", "driver", "dispatch_prepare", "validation", "complete"].includes(row.stage as string)) {
              try {
                const witness = store.armRecoveryWitness();
                const current = store.query("SELECT * FROM work_items WHERE id=?", [row.id as string])[0];
                if (current && canonicalJson(current) === canonicalJson(row)) workWitness = witness;
                else continue;
              } catch { /* Missing witness cannot justify an unknown rejection. */ }
            }
            await process(row);
          } catch { /* No failed or uncertain transition is treated as a business result. */ }
          finally { workWitness = undefined; recoveryScope = undefined; }
        }
        if (closed || rows.length < 16) break;
        const last = rows.at(-1)!; cursor = [last.available_at as number, last.id as string];
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      let cleanupCursor: [number, string] | undefined;
      for (;;) {
        const rows = store.query(`SELECT * FROM work_items WHERE kind='cancel_cleanup' AND stage='pending'${cleanupCursor ? " AND (available_at,id)>(?,?)" : ""} ORDER BY available_at,id LIMIT 16`, [...(cleanupCursor ?? [])]);
        for (const work of rows) {
          if (closed) break;
          try { await cleanup(work); } catch { /* Retry from durable cleanup work on the next pass. */ }
        }
        if (closed || rows.length < 16) break;
        const last = rows.at(-1)!; cleanupCursor = [last.available_at as number, last.id as string];
        await new Promise<void>(resolve => setImmediate(resolve));
      }
    } catch { /* Fenced or frozen storage stops this pass. */ } finally { scanning = false; }
  }
  function requestScan() { if (!closed && !scanning && !scheduled) scheduled = setImmediate(() => { void scan(); }); }
  function canAdvance(runID: string) { try { return !store.effectiveCancellation({ kind: "run", id: runID }); } catch { return false; } }
  return { inspectRecovery() {
    return { inFlightRunIDs: [...transitionsInFlight], quarantinedRunIDs: [...quarantined],
      unknownOperations: [...unknownCores].map(([runID, operationID]) => ({ runID, operationID })),
      haltReason: pausedControlOperation ? "CONTROL_EVIDENCE_MISMATCH" : pausedNonCoreOperation ? "COMMIT_EVIDENCE_MISMATCH"
        : unknownCores.size ? "CORE_CONFIRMATION_UNKNOWN" : quarantined.size ? "RECOVERY_EVIDENCE_UNAVAILABLE" : null,
      pausedOperationID: pausedControlOperation ?? pausedNonCoreOperation ?? null };
  }, canDrive(runID: string) { const state = cores.get(runID)?.getSnapshot()?.state; return !closed && !quarantined.has(runID) && !transitionsInFlight.has(runID) && canAdvance(runID) && (state === "started" || state === "resumed"); }, canWake(runID: string) {
    if (closed || quarantined.has(runID) || transitionsInFlight.has(runID)) return false;
    if (!canAdvance(runID)) return false;
    const row = store.query("SELECT core_json,business_terminal,business_cancelled FROM runs WHERE id=?", [runID])[0];
    if (!row?.core_json || row.business_terminal !== 0 || row.business_cancelled !== 0) return false;
    try { const snapshot = restoreRunCore(decode(row.core_json)).getSnapshot(); return !!snapshot && snapshot.state === "suspended" && snapshot.runID === runID; } catch { return false; }
  }, canExecuteAction(runID: string) {
    if (closed || quarantined.has(runID) || transitionsInFlight.has(runID)) return false;
    if (!canAdvance(runID)) return false;
    const active = cores.get(runID)?.getSnapshot();
    if (active) return active.state === "started" || active.state === "resumed" || active.state === "suspended";
    const persisted = store.query("SELECT core_json FROM runs WHERE id=?", [runID])[0];
    if (!persisted?.core_json) return false;
    try { const snapshot = restoreRunCore(decode(persisted.core_json)).getSnapshot(); return !!snapshot && snapshot.state === "suspended" && snapshot.runID === runID; } catch { return false; }
  }, inspectCore(runID: string) { const snapshot = cores.get(runID)?.getSnapshot(); return { lifecycle: snapshot?.state ?? null, quarantined: quarantined.has(runID) }; }, start() { if (!timer && !closed) {
    store.assertAuthority();
    for (const item of store.query(`SELECT w.id,w.subject_id,w.stage,w.result_json,r.frame_count,r.core_json FROM work_items w JOIN runs r ON r.id=w.subject_id
      WHERE w.kind='run' AND ((w.stage='cancel_control' AND w.result_json IS NOT NULL)
        OR (w.stage IN ('cancel','cancel_control') AND r.frame_count=0 AND json_extract(r.core_json,'$.state')='started'))`)) {
      if (item.stage === "cancel_control" && typeof item.result_json === "string") coldControlIntents.set(item.id as string, item.result_json);
      if (item.frame_count === 0 && item.core_json !== null) coldControlStarts.add(item.subject_id as string);
    }
    requestScan(); timer = setInterval(requestScan, options.pollMs); timer.unref();
  } }, close() { closed = true; if (timer) clearInterval(timer); if (scheduled) clearImmediate(scheduled); } };
}
function isUncertain(error: unknown): boolean { return !!error && typeof error === "object" && "info" in error && (error.info as { disposition?: string }).disposition === "uncertain"; }
