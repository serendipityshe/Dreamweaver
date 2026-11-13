import { encodeRunInput, expectedRunSnapshot } from "../persistence/index.js";
import { compactReportPayload, compactPolicyPayload } from "../contract/index.js";
import { policyPayloadSource, storedPolicySource, compactActionRequestPayload } from "../persistence/index.js";
import { readRunSnapshot } from "../persistence/index.js";
import { readValidationConclusionAsync } from "../persistence/index.js";
import { readProjectedJsonSync, encodeActionSnapshot } from "../persistence/index.js";
import { createHash, randomUUID } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, copyJson, parseJson, validateReport, type ActionObservation, type ActionView, type AppliedPolicy, type CapacityResult, type Fact, type Json, type Outcome, type Report, type RunView } from "../contract/index.js";
import { actionControlDigest, actionControlWrite, actionReportValueReference, makeActionControl, readActionReport, readActionSnapshot, readInlineActionSnapshot, readFactOutcome, reportActionSnapshotProjection, reportProjectionWrites, cancellationGuard, type CommitBatch, type HistoryEvent, type SQLiteStore, type SQLWrite } from "../persistence/index.js";
import { BusinessLeaseService, type LeaseGrant } from "./lease.js";
import { TrustedReportSources, type TrustedReportSource } from "./trusted-report-sources.js";
import { reconciledEffectGuard, resolvedEffectGuard } from "./action-effect.js";
import { verifyRemoteCompletion } from "./remote-completion.js";
import { waitResolutionEvents } from "./wait-history.js";

type Row = ReturnType<SQLiteStore["query"]>[number];
const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
type OutcomeReport = Report<CapacityResult | import("../contract/index.js").Reconciliation>;

/** Coordinator-owned report processing; accepting a source is not itself settlement. */
export function createReportService(store: SQLiteStore, options: { clock: { now(): number }; pollMs: number; prepareTimeoutMs: number; retryAfterMs: number;
  authorizePrincipal(principalID: string, method: "client.submit", kind: "agent" | "capacity", ref: { id: string; version?: string }): void;
  authorizeWorker(grant: LeaseGrant): void; authorizeOperator(principalID: string, request: Json, action: ActionView): void; canDispatchRun(runID: string): boolean; authorizeRecheck?: (actionID: string) => SQLWrite;
  maxResponseBytes?: number }) {
  const leases = new BusinessLeaseService(store, { clock: options.clock, ttlMs: 1, authorizeRecheck: options.authorizeRecheck });
  const trustedSources = new TrustedReportSources(store, options.authorizeOperator);
  let closed = false, scanning = false, timer: ReturnType<typeof setInterval> | undefined, scheduled: ReturnType<typeof setImmediate> | undefined;
  const now = () => options.clock.now();
  const commit = (batch: CommitBatch) => store.commit(batch);
  const event = (type: string, subjectID: string, data: unknown): HistoryEvent => ({ type, subjectID, data: json(data) });
  const makeObservation = (observation: ActionObservation): ActionObservation => observation;

  /** Private evidence-backed intake; public management and child creation remain separate tasks. */
  function acceptTrusted(source: TrustedReportSource): { observationID: string } {
    if (source.source === "operator" && source.outcome.kind === "succeeded") throw new Error("Operator success requires fixed output validation before observation");
    const digest = createHash("sha256").update(canonicalJson([source.source, source.sourceID])).digest("hex");
    const operationID = `trusted-observation:${digest}`, observationID = `trusted:${digest}`;
    const row = store.query("SELECT * FROM actions WHERE id=?", [source.actionID])[0];
    if (!row?.snapshot_json) throw new Error("Trusted source Action is unavailable");
    const action = readInlineActionSnapshot(store, row);
    const reported = json({ payload: source.outcome, evidence: source.evidence, measurements: source.measurements });
    const prior = store.getCommit(operationID);
    const semantic = { source: source.source, sourceID: source.sourceID, actionID: source.actionID, outcome: source.outcome, evidence: source.evidence, measurements: source.measurements };
    if (prior) {
      const history = store.query("SELECT request_json FROM history WHERE operation_id=?", [operationID])[0];
      if (!history || history.request_json !== canonicalJson(semantic)) throw new Error("Trusted source content conflicts with its prior observation");
      return prior.result as unknown as { observationID: string };
    }
    const observation = makeObservation({ id: observationID, attemptID: null, source: source.source, observedAt: now(), result: source.outcome,
      evidence: source.evidence, validationError: null, measurements: source.measurements, reported, validated: true, acceptedAsOutcome: false });
    const next: ActionView = { ...action, latestObservation: observation, revision: action.revision + 1, updatedAt: now() };
    const writes: SQLWrite[] = [...source.guards, write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [next.revision, { json: encodeActionSnapshot(next) }, action.id, action.revision, row.snapshot_json as string]),
      write("INSERT INTO action_observations(id,action_id,attempt_id,source,source_id,snapshot_json) VALUES(?,?,?,?,?,?)", [observationID, action.id, null, source.source, source.sourceID, { json: json(observation) }])];
    for (const [ordinal, sample] of source.measurements.entries()) writes.push(write("INSERT INTO action_measurements(observation_id,ordinal,sample_json) VALUES(?,?,?)", [observationID, ordinal, { json: json(sample) }]));
    return store.commit({ operationID, kind: "report.trusted_observed", subjectID: action.id, request: json(semantic), writes,
      events: [event("action.observed", action.id, { actionID: action.id, observation })], evidence: json({ observationID }), result: json({ observationID }) }).result as unknown as { observationID: string };
  }

  function eligible(authority: LeaseGrant | null, action: ActionView, run: RunView, work: Row | undefined): boolean {
    const recheck = action.latestAttempt?.mode === "recheck-only";
    if (!authority || action.latestAttempt?.id !== authority.attemptID || work?.stage !== "report_pending"
        || !recheck && (action.outcome || action.cancelRequested || run.outcome || run.cancelRequested || !options.canDispatchRun(run.id))) return false;
    try { leases.assert(authority); options.authorizeWorker(authority); options.authorizePrincipal(store.query("SELECT principal_id FROM runs WHERE id=?", [run.id])[0]!.principal_id as string, "client.submit", "capacity", action.request.target); return true; }
    catch { return false; }
  }
  /** Finish coordinator-owned intake work without restoring the reporting
   * worker's authority. A different Attempt, cancellation or terminal state
   * wins over this administrative pause, including races at the write lock. */
  function pendingReportGuards(authority: LeaseGrant | null, action: ActionView, run: RunView, work: Row | undefined): SQLWrite[] | null {
    if (!authority || action.latestAttempt?.id !== authority.attemptID || action.latestAttempt.number !== authority.attemptNumber
        || action.outcome || action.cancelRequested || action.blocked || run.outcome || run.cancelRequested || work?.stage !== "report_pending"
        || store.effectiveCancellation({ kind: "action", id: action.id })) return null;
    const subject = store.query("SELECT epoch FROM actions WHERE id=?", [action.id])[0];
    const lease = store.query("SELECT * FROM business_leases WHERE kind='action' AND subject_id=?", [action.id])[0];
    if (!subject || subject.epoch !== authority.epoch || !lease || lease.epoch !== authority.epoch || lease.node_id !== authority.nodeID
        || lease.worker_principal_id !== authority.workerPrincipalID || lease.incarnation !== authority.incarnation
        || lease.attempt_id !== authority.attemptID || lease.attempt_number !== authority.attemptNumber) return null;
    return [cancellationGuard({ kind: "action", id: action.id }),
      write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND business_cancelled=0 AND business_terminal=0", [run.id]),
      write("UPDATE actions SET epoch=epoch+0 WHERE id=? AND epoch=? AND business_cancelled=0 AND business_terminal=0 AND outcome_json IS NULL", [action.id, authority.epoch]),
      write("UPDATE business_leases SET released=1 WHERE kind='action' AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at=? AND released=? AND attempt_id=? AND attempt_number=?",
        [action.id, authority.nodeID, authority.workerPrincipalID, authority.incarnation, authority.epoch, lease.expires_at as number, lease.released as number, authority.attemptID, authority.attemptNumber])];
  }
  // A persisted report may outlive its read-only recheck lease. Retire only
  // that exact Attempt; a replacement owner or a later report must win the CAS.
  function pendingRecheckGuards(authority: LeaseGrant | null, action: ActionView, work: Row | undefined): SQLWrite[] | null {
    if (!authority || action.latestAttempt?.mode !== "recheck-only" || action.latestAttempt.id !== authority.attemptID
        || action.latestAttempt.number !== authority.attemptNumber || work?.stage !== "report_pending") return null;
    const subject = store.query("SELECT epoch FROM actions WHERE id=?", [action.id])[0];
    const lease = store.query("SELECT * FROM business_leases WHERE kind='action' AND subject_id=?", [action.id])[0];
    if (!subject || subject.epoch !== authority.epoch || !lease || lease.epoch !== authority.epoch || lease.node_id !== authority.nodeID
        || lease.worker_principal_id !== authority.workerPrincipalID || lease.incarnation !== authority.incarnation
        || lease.attempt_id !== authority.attemptID || lease.attempt_number !== authority.attemptNumber) return null;
    return [write("UPDATE actions SET epoch=epoch+0 WHERE id=? AND epoch=?", [action.id, authority.epoch]),
      write("UPDATE action_attempt_state SET stage=? WHERE action_id=? AND attempt_id=? AND mode=? AND stage=? AND execute_dispatched=0", ["reconcile", action.id, authority.attemptID!, "recheck-only", "reconcile"]),
      write("UPDATE business_leases SET released=1 WHERE kind='action' AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at=? AND released=? AND attempt_id=? AND attempt_number=?",
        [action.id, authority.nodeID, authority.workerPrincipalID, authority.incarnation, authority.epoch, lease.expires_at as number, lease.released as number, authority.attemptID, authority.attemptNumber])];
  }
  async function waitSnapshot(run: RunView, newAction: ActionView, outcome: Outcome, factID: string): Promise<{ guards: CommitBatch["factSnapshot"]; writes: SQLWrite[]; events: HistoryEvent[] }> {
    if (!run.wait || run.wait.winner !== null || run.lifecycle !== "suspended") return { guards: [], writes: [], events: [] };
    const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=?", [run.wait.id, run.id])[0];
    const work = store.query("SELECT id,stage FROM work_items WHERE kind=? AND subject_id=?", ["run", run.id])[0];
    if (!wait || wait.winner !== null || !work || work.stage !== "waiting" || wait.action_keys_json !== canonicalJson(run.wait.actionKeys)) return { guards: [], writes: [], events: [] };
    const refs: { actionID: string; actionKey: string; fact: Fact | null }[] = [];
    for (const key of run.wait.actionKeys) {
      const row = store.query("SELECT id FROM actions WHERE run_id=? AND action_key=?", [run.id, key])[0];
      if (!row) return { guards: [], writes: [], events: [] };
      const actionID = row.id as string;
      const existing = store.query("SELECT id,action_id,outcome_json,outcome_projection_id FROM facts WHERE action_id=?", [actionID])[0];
      // Referenced facts are resolved by the Run scheduler, which publishes a
      // witnessed wake and projected history. Rebuilding an inline wake here
      // would duplicate the complete report again. Its factSnapshot CAS also
      // arbitrates timeout versus the newly committed fact.
      if (existing?.outcome_projection_id != null) return { guards: [], writes: [], events: [] };
      refs.push({ actionID, actionKey: key, fact: existing ? { id: existing.id as string, actionID, actionKey: key, outcome: await readFactOutcome(store, existing) } : null });
    }
    const guards = refs.map(item => ({ actionID: item.actionID, factID: item.fact?.id ?? null }));
    const facts = refs.map(item => item.actionID === newAction.id && !item.fact ? { id: factID, actionID: newAction.id, actionKey: item.actionKey, outcome } : item.fact);
    if (facts.some(fact => !fact)) {
      return { guards, writes: [write("UPDATE waits SET winner=NULL WHERE id=? AND run_id=? AND winner IS NULL", [run.wait.id, run.id])], events: [] };
    }
    const wake = { kind: "results", waitID: run.wait.id, facts: facts as Fact[] };
    const updated: RunView = { ...run, wait: { ...run.wait, winner: "results" }, revision: run.revision + 1, updatedAt: now() };
    return { guards, writes: [write("UPDATE waits SET winner=?,wake_json=? WHERE id=? AND run_id=? AND winner IS NULL", ["results", { json: json(wake) }, run.wait.id, run.id]),
      write("UPDATE runs SET revision=?,snapshot_json=? WHERE id=? AND revision=? AND snapshot_json=?", [updated.revision, { json: encodeRunInput(store, updated) }, run.id, run.revision, expectedRunSnapshot(store, run)]),
      write("UPDATE work_items SET stage=?,result_json=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["wake", null, work.id as string, "run", run.id, "waiting"])],
      events: waitResolutionEvents(store, run.id, run.wait.id, facts as Fact[], options.maxResponseBytes) };
  }
  async function process(report: Row) {
    const state = store.query("SELECT * FROM report_processing WHERE report_id=?", [report.id as string])[0];
    if (!state || state.state === "done" || state.state === "deferred") return;
    const actionRow = store.query("SELECT * FROM actions WHERE id=?", [report.action_id as string])[0];
    const runRow = actionRow && store.query("SELECT * FROM runs WHERE id=?", [actionRow.run_id as string])[0];
    const work = actionRow && store.query("SELECT * FROM work_items WHERE kind=? AND subject_id=?", ["action", actionRow.id as string])[0];
    const attempt = store.query("SELECT * FROM action_attempt_state WHERE attempt_id=? AND action_id=?", [report.attempt_id as string, report.action_id as string])[0];
    if (!actionRow?.snapshot_json || !runRow || !work || !attempt) return;
    const action = await readActionSnapshot(store, actionRow), run = readRunSnapshot(store, runRow.snapshot_json);
    const raw = await readActionReport(store, report);
    const reportRef = actionReportValueReference(store, report);
    const recordedAuthority = report.authority_json ? decode<LeaseGrant>(report.authority_json) : null;
    const authority = recordedAuthority?.kind === "action" && recordedAuthority.subjectID === action.id && recordedAuthority.attemptID === report.attempt_id ? recordedAuthority : null;
    let parsed: OutcomeReport | null = null, result: CapacityResult | null = null, failure: { code: string; message: string; details: Json } | null = null;
    if (raw.kind === "report") {
      try {
        parsed = report.phase === "execute" ? validateReport(raw.value, "execute") : validateReport(raw.value, "reconcile");
        if (report.phase === "execute") result = parsed.payload as CapacityResult;
        else if (parsed.payload.kind === "settled") result = parsed.payload.result;
      } catch { failure = { code: "RESULT_CONTRACT_VIOLATION", message: "Capacity returned an invalid report", details: null }; }
    } else failure = raw.code === "CAPACITY_THROWN"
      ? { code: "RECONCILIATION_REQUIRED", message: "Capacity did not return a determinate report", details: { code: raw.code } }
      : { code: "RESULT_CONTRACT_VIOLATION", message: "Capacity report is unavailable or invalid", details: { code: raw.code } };
    let validationError: { code: string; message: string; details: Json } | null = failure;
    let validated = !failure && result?.kind !== "succeeded";
    if (!failure && result?.kind === "succeeded") {
      const accepted = store.query("SELECT data_json FROM history WHERE subject_id=? AND type=? ORDER BY sequence LIMIT 1", [action.id, "action.accepted"])[0];
      if (!accepted) return;
      const frameID = decode<{ frameID: string }>(accepted.data_json).frameID;
      const nativePayload = { kind: "capacity", ref: action.request.target, phase: "report-output", value: reportRef ? null : result.value, config: action.request.config,
        principalID: runRow.principal_id, operation: "client.submit", context: { domainID: store.domainID, rootID: run.rootID, runID: run.id, frameID,
          actionID: action.id, attemptID: report.attempt_id, reportID: report.id, reportPhase: report.phase,
          ...(reportRef ? { reportEncoding: "report-ref-v1", originalReportRef: reportRef } : { originalReport: raw.kind === "report" ? raw.value : null }),
          prepared: action.request, policies: action.request.policies } };
      const originalJob = state.round_id ? store.query("SELECT payload_json FROM validation_jobs WHERE round_id=? LIMIT 1", [state.round_id as string])[0] : undefined;
      const policySource = originalJob ? storedPolicySource((parseJson(originalJob.payload_json as string) as Record<string, Json>).context) : policyPayloadSource(store, nativePayload);
      const payload = compactReportPayload(compactPolicyPayload(compactActionRequestPayload(store, nativePayload, originalJob?.payload_json), policySource as unknown as Json ?? undefined), originalJob?.payload_json);
      if (state.state === "new") {
        const roundID = randomUUID(), at = now(), deadline = at + options.prepareTimeoutMs;
        if (!Number.isSafeInteger(deadline)) return;
        const operationID = `report.prepare:${report.id}`;
        const previous = store.query("SELECT request_json FROM history WHERE operation_id=?", [operationID])[0];
        const previousRequest = previous ? decode<Json>(previous.request_json) : null;
        // Older receipts stored the full request; replay them in their original
        // format so the persistence layer still compares every original byte.
        // New receipts bind the same canonical payload through a fixed digest;
        // the validation job below still persists the complete original value.
        const legacy = previousRequest && typeof previousRequest === "object" && !Array.isArray(previousRequest) && Object.hasOwn(previousRequest, "payload");
        const request = legacy ? { reportID: report.id, payload, timeoutMs: options.prepareTimeoutMs }
          : { version: "sha256-v1", reportID: report.id, payloadDigest: createHash("sha256").update(canonicalJson(payload)).digest("hex"), timeoutMs: options.prepareTimeoutMs };
        commit({ operationID, kind: "report.preparation_created", subjectID: action.id, request: json(request), evidence: json({ roundID, deadline }), result: json({ roundID, deadline }),
          writes: [write("INSERT INTO prepare_rounds (id,owner_kind,owner_id,principal_id,phase,number,prepare_timeout_ms,created_at,deadline_at,state) VALUES (?,?,?,?,?,?,?,?,?,?)", [roundID, "report", report.id as string, runRow.principal_id as string, "report-output", 1, options.prepareTimeoutMs, at, deadline, "pending"]),
            write("UPDATE report_processing SET state=?,round_id=?,revision=revision+1 WHERE report_id=? AND state=? AND revision=?", ["validation", roundID, report.id as string, "new", state.revision as number]),
            write("INSERT INTO validation_jobs (id,round_id,payload_json,state) VALUES (?,?,?,?)", [randomUUID(), roundID, { json: json(payload) }, "pending"])] });
        return;
      }
      const round = store.query("SELECT * FROM prepare_rounds WHERE id=? AND owner_kind=? AND owner_id=? AND state=?", [state.round_id as string, "report", report.id as string, "pending"])[0];
      if (!round) return;
      if (now() >= (round.deadline_at as number)) {
        const writes = [write("UPDATE report_processing SET state=?,revision=revision+1 WHERE report_id=? AND state=? AND revision=?", ["deferred", report.id as string, "validation", state.revision as number]), write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at=?", ["expired", round.id as string, "pending", round.deadline_at as number])];
        const current = eligible(authority, action, run, work);
        const pauseGuards = current ? [...leases.guard(authority!), write("UPDATE business_leases SET released=1 WHERE kind='action' AND subject_id=? AND epoch=? AND released=0", [action.id, authority!.epoch])]
          : pendingRecheckGuards(authority, action, work) ?? pendingReportGuards(authority, action, run, work);
        const events: HistoryEvent[] = [];
        if (pauseGuards) {
          // This state transition is not yet represented by a report-backed
          // snapshot; preserve the existing reference until it can be published.
          if (actionRow.snapshot_projection_id !== null) return;
          const blocked: ActionView = { ...action, revision: action.revision + 1, updatedAt: now(), blocked: { code: "VERSION_UNAVAILABLE", message: "Report output validation expired", details: { reportID: report.id as string, deadlineAt: round.deadline_at as number } } };
          writes.unshift(...pauseGuards, write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [blocked.revision, { json: encodeActionSnapshot(blocked) }, action.id, action.revision, actionRow.snapshot_json as string]),
            write("UPDATE work_items SET stage=? WHERE id=? AND kind='action' AND subject_id=? AND stage=?", ["blocked", work!.id as string, action.id, "report_pending"]));
          events.push(event("action.blocked", action.id, { actionID: action.id, blocked: blocked.blocked }));
        }
        commit({ operationID: `report.defer:${report.id}`, kind: "report.validation_deferred", subjectID: action.id, request: json({ reportID: report.id, roundID: round.id, deadlineAt: round.deadline_at }), writes, events, evidence: null, result: null, ...(current ? { validBefore: authority!.expiresAt } : {}) });
        return;
      }
      const job = store.query("SELECT * FROM validation_jobs WHERE round_id=? AND state=?", [round.id as string, "accepted"])[0];
      if (!job || job.payload_json !== canonicalJson(payload)) return;
      const conclusion = await readValidationConclusionAsync(store, job.report_json);
      if (!conclusion.valid && conclusion.issue?.code !== "OUTPUT_VALIDATION_FAILED") return;
      validated = conclusion.valid;
      validationError = conclusion.valid ? null : { code: "OUTPUT_VALIDATION_FAILED", message: "Capacity output validation failed", details: null };
    }
    const isCurrent = eligible(authority, action, run, work);
    const candidate: Outcome | null = result ? validationError ? { kind: "failed", error: validationError } : result : null;
    const accepted = !!candidate && isCurrent && !action.outcome && !run.outcome && !store.effectiveCancellation({ kind: "action", id: action.id });
    const observation: ActionObservation = makeObservation({ id: report.id as string, attemptID: report.attempt_id as string, source: report.phase as "execute" | "reconcile", observedAt: report.received_at as number,
      result, evidence: parsed?.evidence ?? null, validationError, measurements: parsed?.measurements ?? [], reported: raw.kind === "report" ? raw.value : json(raw), validated,
      acceptedAsOutcome: accepted && !validationError });
    const at = now(), next: ActionView = { ...action, revision: action.revision + 1, updatedAt: at, latestObservation: observation };
    const writes: SQLWrite[] = [write("UPDATE report_processing SET state=?,revision=revision+1 WHERE report_id=? AND state=? AND revision=?", ["done", report.id as string, state.state as string, state.revision as number])];
    // A single original dispatched request can itself resolve the cancellation
    // cleanup's unknown-effect marker. Never clear another owner's block or an
    // unvalidated/conflicting result; recheck the evidence under the write lock.
    const details = action.blocked?.details;
    if (action.outcome?.kind === "cancelled" && action.blocked?.code === "RECONCILIATION_REQUIRED" && details && typeof details === "object" && !Array.isArray(details)
        && details.source === "cancellation" && details.actionID === action.id && details.attemptID === report.attempt_id) {
      const knownEffect = resolvedEffectGuard(store, action, observation);
      if (knownEffect) {
        writes.push(knownEffect);
        next.blocked = null;
      }
    }
    if (state.state === "validation") {
      const round = store.query("SELECT * FROM prepare_rounds WHERE id=?", [state.round_id as string])[0];
      const job = store.query("SELECT * FROM validation_jobs WHERE round_id=? AND state=?", [state.round_id as string, "accepted"])[0];
      if (!round || !job || now() >= (round.deadline_at as number)) return;
      writes.push(write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["accepted", round.id as string, "pending", at]), write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND payload_json=? AND report_json=? AND assignment_id=? AND assignment_epoch=?", ["accepted", job.id as string, "accepted", job.payload_json as string, job.report_json as string, job.assignment_id as string, job.assignment_epoch as number]));
    }
    const projectedObservation = !!(parsed && (reportRef || actionRow.snapshot_projection_id !== null));
    const rawObservation = (reportRef !== null || actionRow.snapshot_projection_id !== null) && raw.kind === "report" && !parsed;
    const observationTemplate = json(projectedObservation ? { ...observation, result: null, evidence: null, measurements: null, reported: null }
      : rawObservation ? { ...observation, reported: null } : observation);
    const observationBindings = [
      ...(projectedObservation ? [
        { reportID: report.id as string, path: ["result"], selector: "result" as const },
        { reportID: report.id as string, path: ["evidence"], selector: "evidence" as const },
        { reportID: report.id as string, path: ["measurements"], selector: "measurements" as const },
      ] : []),
      { reportID: report.id as string, path: ["reported"], selector: "report" as const },
    ];
    const historyProjectionID = `history:observation:${report.id}`;
    let factSnapshot: CommitBatch["factSnapshot"] = [], events: HistoryEvent[] = projectedObservation || rawObservation
      ? [{ type: "action.observed", subjectID: action.id, data: null, dataProjectionID: historyProjectionID }]
      : [event("action.observed", action.id, { actionID: action.id, observation })];
    if (projectedObservation || rawObservation) {
      writes.push(...reportProjectionWrites(historyProjectionID, action.id, json({ actionID: action.id,
        observation: observationTemplate }), observationBindings.map(binding => ({ ...binding, path: ["observation", ...binding.path] }))));
    }
    const continuing = !!(isCurrent && parsed && report.phase === "reconcile" && parsed.payload.kind === "retryable" && attempt.mode === "automatic-recovery" && action.executionCount < run.limits.maxExecutionsPerAction);
    if (isCurrent) {
      if (!continuing && next.latestAttempt?.id === report.attempt_id) next.latestAttempt = { ...next.latestAttempt, finishedAt: at };
      writes.unshift(...leases.guard(authority!));
      if (!continuing) writes.push(write("UPDATE business_leases SET released=1 WHERE kind=? AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at>=? AND released=0 AND attempt_id IS ? AND attempt_number IS ?", [authority!.kind, authority!.subjectID, authority!.nodeID, authority!.workerPrincipalID, authority!.incarnation, authority!.epoch, authority!.expiresAt, authority!.attemptID, authority!.attemptNumber]));
    }
    let projectedOutcome: { id: string; digest: string } | null = null;
    if (accepted && candidate) {
      writes.push(cancellationGuard({ kind: "action", id: action.id }), write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND business_cancelled=0 AND business_terminal=0", [run.id]));
      next.status = "settled"; next.outcome = candidate; next.outcomeObservationID = observation.id; next.settledAt = at; next.blocked = null;
      const factID = randomUUID(), wait = reportRef ? { guards: [], writes: [], events: [] }
        : await waitSnapshot(run, action, candidate, factID);
      factSnapshot = wait.guards;
      const projectedSettlement = !!(reportRef && !validationError);
      if (projectedSettlement) {
        const projectionID = `fact:${factID}`;
        const hash = createHash("sha256");
        for (const part of canonicalJsonChunks(candidate)) { hash.update(part); await new Promise<void>(resolve => setImmediate(resolve)); }
        projectedOutcome = { id: projectionID, digest: hash.digest("hex") };
        writes.push(...reportProjectionWrites(projectionID, action.id, null, [{ reportID: report.id as string, path: [], selector: "result" }]),
          write("INSERT INTO facts(id,run_id,action_id,outcome_json,outcome_projection_id) VALUES(?,?,?,?,?)", [factID, run.id, action.id, { json: null }, projectionID]));
      } else writes.push(write("INSERT INTO facts(id,run_id,action_id,outcome_json) VALUES(?,?,?,?)", [factID, run.id, action.id, { json: json(candidate) }]));
      writes.push(...wait.writes);
      if (projectedSettlement) {
        const projectionID = `history:settlement:${report.id}`;
        writes.push(...reportProjectionWrites(projectionID, action.id, json({ actionID: action.id, outcome: null, sourceObservationID: observation.id, settledAt: at }),
          [{ reportID: report.id as string, path: ["outcome"], selector: "result" }]));
        events.push({ type: "action.settled", subjectID: action.id, data: null, dataProjectionID: projectionID }, ...wait.events);
      } else events.push(event("action.settled", action.id, { actionID: action.id, outcome: candidate, sourceObservationID: observation.id, settledAt: at }), ...wait.events);
      writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["done", work.id as string, "action", action.id, "report_pending"]));
    } else if (isCurrent && attempt.mode === "recheck-only" && (action.outcome || run.outcome || store.effectiveCancellation({ kind: "action", id: action.id }))) {
      // A settled report alone does not prove old requests have stopped. Bind
      // the conclusion to the unknown marker and all execute completion reports.
      const knownEffect = parsed?.payload.kind === "settled" ? await reconciledEffectGuard(store, action, observation) : null;
      if (knownEffect) { writes.push(knownEffect); next.blocked = null; }
      if (parsed?.payload.kind === "unresolved") next.blocked = parsed.payload.reason;
      else if (parsed?.payload.kind === "retryable") next.blocked = { code: "RECONCILIATION_REQUIRED", message: "Reconciliation suggested retry; execution remains prohibited", details: { reportID: report.id as string } };
      writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["done", work.id as string, "action", action.id, "report_pending"]));
    } else if (isCurrent && failure) {
      next.blocked = { code: "RECONCILIATION_REQUIRED", message: "Capacity outcome requires reconciliation", details: { reportID: report.id as string } };
      writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["blocked", work.id as string, "action", action.id, "report_pending"]));
      events.push(event("action.blocked", action.id, { actionID: action.id, blocked: next.blocked }));
    } else if (isCurrent && parsed && report.phase === "reconcile" && parsed.payload.kind === "unresolved") {
      next.blocked = parsed.payload.reason;
      writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["blocked", work.id as string, "action", action.id, "report_pending"]));
      events.push(event("action.blocked", action.id, { actionID: action.id, blocked: next.blocked }));
    } else if (isCurrent && parsed && report.phase === "reconcile" && parsed.payload.kind === "retryable") {
      if (attempt.mode === "automatic-recovery" && action.executionCount < run.limits.maxExecutionsPerAction) {
        const prior = store.query("SELECT report_id,attempt_id,consumed FROM action_retry_authorizations WHERE action_id=?", [action.id])[0];
        writes.push(prior ? write("UPDATE action_retry_authorizations SET report_id=?,attempt_id=?,consumed=0 WHERE action_id=? AND report_id=? AND attempt_id=? AND consumed=?", [report.id as string, report.attempt_id as string, action.id, prior.report_id as string, prior.attempt_id as string, prior.consumed as number]) : write("INSERT INTO action_retry_authorizations(action_id,report_id,attempt_id,consumed) VALUES(?,?,?,0)", [action.id, report.id as string, report.attempt_id as string]));
        writes.push(write("UPDATE action_attempt_state SET stage=? WHERE attempt_id=? AND action_id=? AND mode=? AND stage=? AND execute_dispatched=0", ["execute", report.attempt_id as string, action.id, "automatic-recovery", "reconcile"]),
          write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["running", work.id as string, "action", action.id, "report_pending"]));
      } else if (attempt.mode === "recheck-only") {
        next.blocked = { code: "RECONCILIATION_REQUIRED", message: "Reconciliation suggested retry", details: { reportID: report.id as string } };
        writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["blocked", work.id as string, "action", action.id, "report_pending"]));
      } else {
        next.blocked = { code: "LIMIT_EXCEEDED", message: "Action cannot automatically retry", details: { reportID: report.id as string } };
        writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["blocked", work.id as string, "action", action.id, "report_pending"]));
      }
    }
    if (!isCurrent) {
      const pauseGuards = pendingRecheckGuards(authority, action, work) ?? pendingReportGuards(authority, action, run, work);
      if (pauseGuards) {
        next.blocked = { code: "RECONCILIATION_REQUIRED", message: "Durable report requires authorized resolution after execution authority was lost", details: { reportID: report.id as string, phase: report.phase as string, expiresAt: authority!.expiresAt } };
        next.latestAttempt = { ...action.latestAttempt!, finishedAt: at };
        writes.unshift(...pauseGuards);
        writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind='action' AND subject_id=? AND stage=?", ["blocked", work.id as string, action.id, "report_pending"]));
        events.push(event("action.blocked", action.id, { actionID: action.id, blocked: next.blocked }));
      }
    }
    const previousProjection = actionRow.outcome_projection_id as string | null;
    const outcomeStorage = projectedOutcome ? { json: { kind: next.outcome!.kind } } : previousProjection !== null ? actionRow.outcome_json as string : next.outcome ? { json: json(next.outcome) } : null;
    let snapshotProjection: ReturnType<typeof reportActionSnapshotProjection> = null;
    if (actionRow.snapshot_projection_id !== null || projectedOutcome && report.phase === "execute" && next.outcome?.kind === "succeeded") {
      const diagnostic = raw.kind === "diagnostic" && observation.result === null;
      // Other report shapes require their own durable projection and evidence
      // before they can safely update a referenced Action.
      if (!projectedObservation && !rawObservation && !diagnostic || next.outcome?.kind !== "succeeded" || !next.outcomeObservationID || !projectedOutcome && previousProjection === null) return;
      snapshotProjection = reportActionSnapshotProjection(next, report.id as string, next.outcomeObservationID, actionRow.snapshot_projection_id !== null);
      if (snapshotProjection) writes.push(...snapshotProjection.writes);
    }
    writes.push(write("UPDATE actions SET revision=?,snapshot_json=?,snapshot_projection_id=?,outcome_json=?,outcome_projection_id=? WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS ? AND outcome_json IS ? AND outcome_projection_id IS ?", [next.revision, snapshotProjection ? { json: null } : { json: encodeActionSnapshot(next) }, snapshotProjection?.id ?? null, outcomeStorage, projectedOutcome?.id ?? previousProjection, action.id, action.revision, actionRow.snapshot_json as string, actionRow.snapshot_projection_id as string | null, actionRow.outcome_json as string | null, previousProjection]));
    if (projectedObservation || rawObservation) {
      const projectionID = `observation:${report.id}`;
      writes.push(...reportProjectionWrites(projectionID, action.id, observationTemplate, observationBindings), write("INSERT INTO action_observations(id,action_id,attempt_id,source,source_id,snapshot_json,snapshot_projection_id) VALUES(?,?,?,?,?,?,?)",
        [observation.id, action.id, observation.attemptID, observation.source, report.id as string, { json: null }, projectionID]));
    } else writes.push(write("INSERT INTO action_observations(id,action_id,attempt_id,source,source_id,snapshot_json) VALUES(?,?,?,?,?,?)", [observation.id, action.id, observation.attemptID, observation.source, report.id as string, { json: json(observation) }]));
    const control = projectedObservation || rawObservation || snapshotProjection ? makeActionControl(next) : null;
    if (control) writes.push(actionControlWrite(`report.observe:${report.id}`, control));
    for (const [ordinal, sample] of observation.measurements.entries()) writes.push(write("INSERT INTO action_measurements(observation_id,ordinal,sample_json) VALUES(?,?,?)", [observation.id, ordinal, { json: json(sample) }]));
    const validBefore = isCurrent ? Math.min(authority!.expiresAt, state.state === "validation" ? (store.query("SELECT deadline_at FROM prepare_rounds WHERE id=?", [state.round_id as string])[0]!.deadline_at as number) : Number.MAX_SAFE_INTEGER) : state.state === "validation" ? (store.query("SELECT deadline_at FROM prepare_rounds WHERE id=?", [state.round_id as string])[0]!.deadline_at as number) : undefined;
    if (isCurrent && !eligible(authority, action, run, work)) return;
    // A referenced report already binds the original immutable bytes through
    // action.report_received. Repeating its payload four times in the commit
    // identity would reintroduce the single-string/SQLite-row limit despite
    // chunked intake. Keep every independently derived field in the identity,
    // and explicitly bind each omitted field to the original report value.
    const observationRequest = projectedObservation || rawObservation || snapshotProjection ? {
      version: "report-observation-ref-v1", reportID: report.id, reportValue: reportRef,
      observation: observationTemplate,
      ...(projectedObservation ? { reportFields: { result: "result", evidence: "evidence", measurements: "measurements", reported: "report" } } : {}),
      candidate: accepted ? validationError ? candidate : { kind: "report-result" } : null,
      actionControlDigest: actionControlDigest(control!),
      ...(snapshotProjection ? { snapshotProjectionID: snapshotProjection.id, snapshotSource: snapshotProjection.source, actionControl: control } : {}),
      ...(projectedOutcome ? { outcomeProjectionID: projectedOutcome.id, outcomeDigest: projectedOutcome.digest } : {}),
    } : { reportID: report.id, observation, candidate: accepted ? candidate : null };
    commit({ operationID: `report.observe:${report.id}`, kind: "report.observed", subjectID: action.id, request: json(observationRequest),
      writes, events, evidence: json({ observationID: observation.id }), result: json({ observationID: observation.id }), ...(factSnapshot?.length ? { factSnapshot } : {}), ...(validBefore === undefined ? {} : { validBefore }) });
  }
  async function scan() {
    scheduled = undefined; if (closed || scanning) return; scanning = true;
    try {
      let cursor = "";
      for (;;) {
        store.assertAuthority();
        // A pending validation without an accepted job cannot advance before
        // its deadline. Inspect only small metadata until it is actionable;
        // repeatedly reading every full report here can starve lease renewals.
        // Missing or inconsistent rounds stay eligible for the existing
        // processor to handle conservatively, rather than being hidden.
        const rows = store.query(`SELECT p.report_id AS id FROM report_processing p
          WHERE p.state IN ('new','validation') AND p.report_id>?
            AND (p.state='new' OR NOT EXISTS (
              SELECT 1 FROM prepare_rounds q WHERE q.id=p.round_id AND q.owner_kind='report' AND q.owner_id=p.report_id
                AND q.state='pending' AND q.deadline_at>?
                AND NOT EXISTS (SELECT 1 FROM validation_jobs j WHERE j.round_id=q.id AND j.state='accepted')
            )) ORDER BY p.report_id LIMIT 16`, [cursor, now()]);
        for (const item of rows) {
          if (closed) break;
          try { const row = store.query("SELECT * FROM action_reports WHERE id=?", [item.id as string])[0]; if (row) await process(row); }
          catch { /* Failed CAS, fencing or uncertain commit never fabricates a result. */ }
          // Do not materialize a page of large reports or monopolize the loop
          // while advancing independent report identities.
          await new Promise<void>(resolve => setImmediate(resolve));
        }
        if (closed || rows.length < 16) break;
        cursor = rows.at(-1)!.id as string; await new Promise<void>(resolve => setImmediate(resolve));
      }
    } catch { /* Lost coordinator authority ends this scan. */ } finally { scanning = false; }
  }
  function requestScan() { if (!closed && !scanning && !scheduled) scheduled = setImmediate(() => { void scan(); }); }
  return { acceptTrustedOperator(input: { principalID: string; requestKey: string }) { return acceptTrusted(trustedSources.operator(input)); },
    acceptTrustedRemote(input: { actionID: string; childRunID: string }) {
      const source = trustedSources.remote(input);
      const proof = store.query("SELECT operation_id,request_json FROM history WHERE subject_id=? AND type='core.complete' AND operation_id IS NOT NULL", [input.childRunID])[0];
      const request = proof && decode<Json>(proof.request_json);
      const remote = request && typeof request === "object" && !Array.isArray(request) ? request.remote : null;
      if (remote) {
        const childRow = store.query("SELECT snapshot_json FROM runs WHERE id=?", [input.childRunID])[0];
        if (!childRow || !verifyRemoteCompletion(store, readRunSnapshot(store, childRow.snapshot_json), proof!.operation_id as string, request!)) throw new Error("Remote atomic completion proof is unavailable");
        const observation = store.query("SELECT id,snapshot_json,snapshot_projection_id FROM action_observations WHERE source='remote' AND source_id=? AND action_id=?", [input.childRunID, input.actionID])[0];
        if (!observation) throw new Error("Remote atomic observation is unavailable");
        const view = readProjectedJsonSync(store, observation.snapshot_json, observation.snapshot_projection_id, input.actionID) as unknown as ActionObservation;
        if (canonicalJson(view.result) !== canonicalJson(source.outcome) || canonicalJson(view.evidence) !== canonicalJson(source.evidence)) throw new Error("Remote observation conflicts with completion evidence");
        return { observationID: observation.id as string };
      }
      return acceptTrusted(source);
    },
    start() { if (!timer && !closed) { requestScan(); timer = setInterval(requestScan, options.pollMs); timer.unref(); } }, close() { closed = true; if (timer) clearInterval(timer); if (scheduled) clearImmediate(scheduled); } };
}
