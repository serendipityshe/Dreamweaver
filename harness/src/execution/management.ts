import { encodeRunInput, sameChildValue } from "../persistence/index.js";
import { stageCommandEvidence, encodeCommandEvidence, readCommandEvidenceRequest, commandEvidenceProjection, commandPublicationWrites, abandonCommandValue, type CommandEvidenceSource } from "../persistence/index.js";
import { compactDispatchPayload } from "../contract/index.js";
import { readPreparationPolicies, storedPolicySource } from "../persistence/index.js";
import { agentOutputValidation, readFrameDecision, readPreparationOriginals } from "../persistence/index.js";
import { readRunSnapshot } from "../persistence/index.js";
import { createHash, randomUUID } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, HarnessError, jsonEqual, parseJson, validateDecision,
  type ErrorInfo, type Json, type ResumeRunRequest, type RunView } from "../contract/index.js";
import { authorizeOperation, type PrincipalGrant } from "../message/index.js";
import { cancellationGuard, type SQLWrite, type SQLiteStore } from "../persistence/index.js";

const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });

/** Administrative resumption retains the original frame and its candidate decision. */
export function createManagementService(store: SQLiteStore, options: {
  clock: { now(): number }; prepareTimeoutMs: number;
  valueQuota?: number;
  authorizeRun(id: string, principal: PrincipalGrant): RunView;
  authorizeRunControl?(id: string, principal: PrincipalGrant): { revision: number };
}) {
    function apply(request: ResumeRunRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant, source: CommandEvidenceSource | null = null): RunView {
      const storedRequest = encodeCommandEvidence(request, source);
      for (;;) {
        store.assertAuthority();
        const grant = revalidate();
        authorizeOperation(grant, "admin.resumeRun");
        const existing = store.query("SELECT * FROM commands WHERE principal_id=? AND request_key=?", [principal.principalID, request.requestKey])[0];
        if (existing) {
          const original = readCommandEvidenceRequest<ResumeRunRequest>(store, existing.request_json);
          options.authorizeRun(existing.operation === "resumeRun" ? original.runID : request.runID, grant);
          if (existing.operation !== "resumeRun" || !sameChildValue(original, request)) throw harnessError("REQUEST_CONFLICT", "Request key is bound to another command");
          if (existing.status === "rejected" && existing.error_json !== null) throw new HarnessError(decode<ErrorInfo>(existing.error_json));
          if (existing.status !== "applied" || existing.result_json === null) throw harnessError("INTERNAL_ERROR", "Resume receipt is unavailable", null, "uncertain");
          return readRunSnapshot(store, existing.result_json);
        }
        const projected = options.authorizeRun(request.runID, grant);
        const row = store.query("SELECT * FROM runs WHERE id=?", [request.runID])[0];
        if (!row) throw harnessError("NOT_FOUND", "Run not found");
        const view = readRunSnapshot(store, row.snapshot_json);
        if (view.revision !== row.revision || projected.revision !== view.revision) continue;
        const failure = view.outcome || projected.cancelRequested ? harnessError("RUN_TERMINAL", "Run is terminal or cancelled")
          : view.revision !== request.expectedRevision ? harnessError("REVISION_CONFLICT", "Run revision changed")
          : !view.blocked ? harnessError("STATE_CONFLICT", "Run is not blocked")
          : !["VERSION_UNAVAILABLE", "FORBIDDEN", "STATE_CONFLICT"].includes(view.blocked.code) ? harnessError("STATE_CONFLICT", "Run cannot resume a permanent block") : null;
        const reject = (reason: HarnessError): boolean => {
          const at = options.clock.now();
          if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock");
          try {
            store.commit({ operationID: `resume:${canonicalJson([principal.principalID, request.requestKey])}`, kind: "run.resume_rejected", subjectID: view.id, request: storedRequest, result: null, evidence: json(reason.info),
              writes: [
                ...(!view.outcome && !projected.cancelRequested ? [cancellationGuard({ kind: "run", id: view.id })] : []),
                write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [view.id, view.revision, row.snapshot_json as string]),
                write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", [principal.principalID, request.requestKey, "resumeRun", { json: storedRequest }, canonicalJsonDigest(request), "rejected", null, { json: json(reason.info) }, at, at]),
                ...commandPublicationWrites(source, store.coordinatorEpoch),
              ], ...(source ? { valuePublications: [source.value] } : {}) });
            return true;
          } catch (error) {
            if (error instanceof HarnessError && error.info.disposition !== "uncertain" && ["REVISION_CONFLICT", "EXECUTION_FENCED", "REQUEST_CONFLICT"].includes(error.info.code)) return false;
            throw error;
          }
        };
        if (failure) { if (!reject(failure)) continue; throw failure; }
        if (!view.blocked) throw harnessError("INTERNAL_ERROR", "Blocked Run state is unavailable");
        const work = store.query("SELECT * FROM work_items WHERE kind='run' AND subject_id=?", [view.id])[0];
        if (!work || work.stage !== "blocked") {
          const reason = harnessError("STATE_CONFLICT", "Run control work is not blocked");
          if (!reject(reason)) continue;
          throw reason;
        }
        const at = options.clock.now();
        if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock");
        const frame = store.query("SELECT * FROM frames WHERE run_id=? AND state='open' ORDER BY number LIMIT 1", [view.id])[0];
        const candidate = frame && store.query("SELECT * FROM frame_candidates WHERE frame_id=?", [frame.id as string])[0];
        const writes: SQLWrite[] = [cancellationGuard({ kind: "run", id: view.id })];
        let nextStage: string;
        try {
        if (frame && candidate) {
          const decision = readFrameDecision(store, frame.id as string, candidate.decision_json);
          const previous = store.query("SELECT * FROM prepare_rounds WHERE owner_kind='frame' AND owner_id=? ORDER BY number DESC LIMIT 1", [frame.id as string])[0];
          if (!previous || previous.state !== "expired" || !["VERSION_UNAVAILABLE", "FORBIDDEN"].includes(view.blocked.code)) throw harnessError("STATE_CONFLICT", "The candidate cannot be resumed safely");
          const deadline = at + options.prepareTimeoutMs, roundID = randomUUID();
          if (!Number.isSafeInteger(deadline)) throw harnessError("LIMIT_EXCEEDED", "Preparation deadline exceeds supported range");
          const phase = decision.kind === "dispatch" ? "dispatch" : "output";
          if (previous.phase !== phase || decision.kind === "complete" && decision.outcome.kind !== "succeeded") throw harnessError("STATE_CONFLICT", "Preparation does not match the candidate");
          writes.push(write("UPDATE frames SET state='open' WHERE id=? AND run_id=? AND state='open' AND input_json=?", [frame.id as string, view.id, frame.input_json as string]),
            write("INSERT INTO prepare_rounds(id,owner_kind,owner_id,principal_id,phase,number,prepare_timeout_ms,created_at,deadline_at,state) VALUES(?,?,?,?,?,?,?,?,?,?)", [roundID, "frame", frame.id as string, row.principal_id as string, phase, (previous.number as number) + 1, options.prepareTimeoutMs, at, deadline, "pending"]));
          if (decision.kind === "dispatch") {
            const prep = store.query("SELECT * FROM dispatch_preparations WHERE frame_id=? AND round_id=?", [frame.id as string, previous.id as string])[0];
            if (!prep) throw harnessError("STATE_CONFLICT", "Original dispatch preparation is unavailable");
            const policies = readPreparationPolicies(store, prep), policySource = storedPolicySource(prep.policies_json), original = readPreparationOriginals(store, prep);
            const rewrite = original.length > 0 && policies.length > 0;
            writes.push(write("UPDATE dispatch_preparations SET round_id=?,rewritten_json=?,resolved_json=NULL,pointer_revisions_json=NULL,state=?,generation=generation+1 WHERE frame_id=? AND round_id=? AND state=? AND generation=?", [roundID, rewrite ? null : { json: parseJson(prep.original_json as string) }, rewrite ? "rewrite" : "resolve", frame.id as string, previous.id as string, prep.state as string, prep.generation as number]));
            if (rewrite) {
              const payload = { kind: "agent", ref: view.agent, phase: "rewrite", value: original, config: null, principalID: row.principal_id,
                operation: "client.submit", context: { domainID: store.domainID, rootID: view.rootID, runID: view.id, frameID: frame.id, decision, policies, ...(policySource ? { policySource } : {}) } };
              for (const action of original) writes.push(write("INSERT INTO validation_jobs(id,round_id,payload_json,state) VALUES(?,?,?,?)", [randomUUID(), roundID, { json: compactDispatchPayload({ ...payload, value: [action] }) }, "pending"]));
            }
            nextStage = "dispatch_prepare";
          } else {
            if (decision.outcome.kind !== "succeeded") throw harnessError("STATE_CONFLICT", "Output preparation has no successful value");
            const payload = agentOutputValidation(store, view, frame.id as string, row.principal_id as string, decision);
            writes.push(write("INSERT INTO validation_jobs(id,round_id,payload_json,state) VALUES(?,?,?,?)", [randomUUID(), roundID, { json: compactDispatchPayload(payload) }, "pending"]));
            nextStage = "validation";
          }
        } else if (!frame && view.lifecycle === null) {
          nextStage = "start";
        } else if (frame && !candidate && ["VERSION_UNAVAILABLE", "FORBIDDEN", "STATE_CONFLICT"].includes(view.blocked.code)) {
          const job = store.query("SELECT * FROM driver_jobs WHERE id=? AND run_id=?", [frame.id as string, view.id])[0];
          const report = job?.report_json ? decode<{ ok: boolean; issue?: { code?: string; details?: { phase?: string } } }>(job.report_json) : null;
          const retryableReport = job?.state === "accepted" && report?.ok === false && report.issue?.details?.phase === "execution" && report.issue.code === view.blocked.code;
          if (!job || job.coordinator_epoch !== store.coordinatorEpoch || !(job.state === "superseded" && view.blocked.code === "VERSION_UNAVAILABLE" || retryableReport)) throw harnessError("STATE_CONFLICT", "Original Driver work cannot be resumed safely");
          writes.push(write("UPDATE driver_jobs SET state='pending',assignment_id=NULL,assignment_epoch=0,node_incarnation=NULL,assignment_deadline_at=NULL,report_json=NULL WHERE id=? AND run_id=? AND state=? AND assignment_epoch=? AND report_json IS ?", [frame.id as string, view.id, job.state as string, job.assignment_epoch as number, job.report_json as string | null]));
          nextStage = "driver";
        } else throw harnessError("STATE_CONFLICT", "Run block cannot be resumed safely");
        } catch (error) {
          if (error instanceof HarnessError && error.info.disposition === "rejected" && ["STATE_CONFLICT", "LIMIT_EXCEEDED"].includes(error.info.code)) {
            if (!reject(error)) continue;
          }
          throw error;
        }
        const updated: RunView = { ...view, revision: view.revision + 1, updatedAt: at, blocked: null };
        let result = updated;
        if (view.rootID !== view.id) {
          const root = store.query("SELECT revision,snapshot_json FROM runs WHERE id=?", [view.rootID])[0];
          if (!root?.snapshot_json) throw harnessError("INTERNAL_ERROR", "Root Run is unavailable");
          const rootView = readRunSnapshot(store, root.snapshot_json);
          if (rootView.id !== view.rootID || rootView.rootID !== view.rootID || rootView.revision !== root.revision) throw harnessError("INTERNAL_ERROR", "Root Run is inconsistent");
          result = { ...updated, rootActionCount: rootView.rootActionCount };
          writes.push(write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [rootView.id, rootView.revision, root.snapshot_json as string]));
        }
        writes.push(write("UPDATE runs SET revision=?,snapshot_json=?,epoch=epoch+1 WHERE id=? AND revision=? AND snapshot_json=? AND business_cancelled=0 AND business_terminal=0", [updated.revision, { json: encodeRunInput(store, updated) }, view.id, view.revision, row.snapshot_json as string]),
          write("UPDATE work_items SET stage=?,available_at=? WHERE id=? AND kind='run' AND subject_id=? AND stage='blocked'", [nextStage, at, work.id as string, view.id]),
          write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", [principal.principalID, request.requestKey, "resumeRun", { json: storedRequest }, canonicalJsonDigest(request), "applied", { json: encodeRunInput(store, result) }, null, at, at]), ...commandPublicationWrites(source, store.coordinatorEpoch));
        try {
          return readRunSnapshot(store, store.commit({ operationID: `resume:${canonicalJson([principal.principalID, request.requestKey])}`, kind: "run.resume_requested", subjectID: view.id, request: storedRequest, writes, result: encodeRunInput(store, result), evidence: commandEvidenceProjection(source, request.evidence, [[]]),
            ...(source ? { valuePublications: [source.value] } : {}),
            events: [{ type: "run.resumed_by_operator", subjectID: view.id, data: commandEvidenceProjection(source, { principalID: principal.principalID, requestKey: request.requestKey, evidence: request.evidence }, [["evidence"]]) }] }).result);
        } catch (error) {
          if (error instanceof HarnessError && error.info.disposition !== "uncertain" && ["REVISION_CONFLICT", "EXECUTION_FENCED", "REQUEST_CONFLICT"].includes(error.info.code)) continue;
          throw error;
        }
      }
    }
  return { resumeRun(request: ResumeRunRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant = () => principal): RunView | Promise<RunView> {
    let bytes = 0;
    for (const chunk of canonicalJsonChunks(request.evidence)) { bytes += chunk.length; if (bytes > 16_384) break; }
    if (bytes <= 16_384 || store.query("SELECT request_key FROM commands WHERE principal_id=? AND request_key=?", [principal.principalID, request.requestKey])[0]) return apply(request, principal, revalidate);
    request = copyJson(request) as unknown as ResumeRunRequest;
    return (async () => {
      const check = () => { const fresh = revalidate(); if (fresh.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Resume principal changed"); authorizeOperation(fresh, "admin.resumeRun"); (options.authorizeRunControl ?? options.authorizeRun)(request.runID, fresh); };
      const source = await stageCommandEvidence(store, principal.principalID, "resumeRun", request, request.runID, `resume:${canonicalJson([principal.principalID, request.requestKey])}`, options.valueQuota ?? Number.MAX_SAFE_INTEGER, check);
      try { check(); return apply(request, principal, revalidate, source); } finally { abandonCommandValue(store, source); }
    })();
  } };
}
