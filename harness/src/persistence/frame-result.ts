import { readFrameDecision, readFrameDecisionAsync } from "./driver-value.js";
import { canonicalValueWitness } from "./canonical-value.js";
import { createHash } from "node:crypto";
import { canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, type Decision, type Json } from "../contract/index.js";
import type { SQLiteStore, SQLWrite } from "./sqlite.js";

export interface FrameResultSource { runID: string; frameID: string; operationID: string; digest: string }
type Row = Record<string, unknown>;
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid completed frame result source"); }

/** Bind a projection's empty slots to one immutable completed decision. The
 * publication joins the child Core receipt in the caller's single transaction. */
export function frameResultProjectionWrites(id: string, actionID: string, template: Json, paths: (string | number)[][], source: FrameResultSource): SQLWrite[] {
  if (!paths.length) invalid();
  return [
    { sql: "INSERT INTO value_projections(id,action_id,encoding,binding_count,template_json) VALUES(?,?,?,?,?)", params: [id, actionID, "report-projection-v1", paths.length, { json: copyJson(template) }], expectChanges: 1 },
    { sql: "INSERT INTO frame_result_projections(projection_id,run_id,frame_id,operation_id,digest,paths_json) VALUES(?,?,?,?,?,?)",
      params: [id, source.runID, source.frameID, source.operationID, source.digest, { json: copyJson(paths) }], expectChanges: 1 },
  ];
}

/** Does not confer coordinator authority. Callers retain their normal read or
 * recovery gate; startup may use it only to validate retained content. */
export function readFrameResult(store: SQLiteStore, source: Row, actionID?: string): Json {
  const steps = frameResultSteps(store, source, actionID);
  let step = steps.next();
  while (!step.done) {
    const job = step.value;
    if (job.kind === "decision") step = steps.next(readFrameDecision(store, job.frameID, job.encoded));
    else {
      const hash = createHash("sha256"); let length = 0;
      for (const part of canonicalJsonChunks(job.value)) { length += part.length; hash.update(part); }
      step = steps.next({ length, digest: hash.digest("hex") });
    }
  }
  return step.value;
}

export async function readFrameResultAsync(store: SQLiteStore, source: Row, actionID?: string, evidence = false): Promise<Json> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assert(); const epoch = store.coordinatorEpoch;
  const steps = frameResultSteps(store, source, actionID);
  let step = steps.next();
  while (!step.done) {
    const job = step.value;
    const result = job.kind === "decision" ? await readFrameDecisionAsync(store, job.frameID, job.encoded, evidence) : await canonicalValueWitness(job.value);
    assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Frame result read authority changed");
    step = steps.next(result);
  }
  return step.value;
}

type ResultRead = { kind: "decision"; frameID: string; encoded: unknown } | { kind: "witness"; value: unknown };
type Witness = { length: number; digest: string };
function* frameResultSteps(store: SQLiteStore, source: Row, actionID?: string): Generator<ResultRead, Json, Decision | Witness> {
  if (typeof source.run_id !== "string" || typeof source.frame_id !== "string" || typeof source.operation_id !== "string" || typeof source.digest !== "string") invalid();
  const row = store.query(`SELECT c.decision_json,f.state,r.snapshot_json,r.core_json,h.request_json,h.data_json
    FROM frame_candidates c JOIN frames f ON f.id=c.frame_id
    JOIN runs r ON r.id=f.run_id
    JOIN history h ON h.operation_id=? AND h.type='core.complete' AND h.subject_id=r.id
    WHERE c.frame_id=? AND r.id=?`, [source.operation_id, source.frame_id, source.run_id])[0];
  if (!row || row.state !== "committed") invalid();
  if (actionID !== undefined && !store.query("SELECT action_id FROM remote_run_links WHERE action_id=? AND child_run_id=?", [actionID, source.run_id])[0]) invalid();
  const decision = (yield { kind: "decision", frameID: source.frame_id as string, encoded: row.decision_json }) as Decision;
  if (decision.kind !== "complete") invalid();
  const outcomeWitness = (yield { kind: "witness", value: decision.outcome }) as Witness;
  const request = parseJson(row.request_json as string) as Record<string, Json>;
  const run = parseJson(row.snapshot_json as string) as Record<string, Json>;
  const proposal = request.proposal as Record<string, Json> | undefined;
  const core = parseJson(row.core_json as string) as Record<string, Json>;
  const after = proposal?.after as Record<string, Json> | undefined;
  if (!proposal || request.frameID !== source.frame_id || proposal.operationID !== source.operation_id || proposal.action !== "complete"
      || canonicalJsonDigest(proposal) !== canonicalJsonDigest(parseJson(row.data_json as string))
      || canonicalJsonDigest(after) !== canonicalJsonDigest(core) || core.runID !== source.run_id || core.state !== "completed"
      || core.outcome !== decision.outcome.kind || run.id !== source.run_id || run.lifecycle !== "completed"
      || actionID !== undefined && run.parentActionID !== actionID || outcomeWitness.digest !== source.digest) invalid();
  if (run.snapshotFormat === "run-outcome-ref-v1") {
    const ref = run.outcomeSource as Record<string, Json>, outcome = run.outcome as Record<string, Json>;
    if (!ref || !outcome || Object.keys(outcome).join(",") !== "kind" || outcome.kind !== decision.outcome.kind
        || ref.frameID !== source.frame_id || ref.operationID !== source.operation_id || ref.digest !== source.digest) invalid();
  } else if (((yield { kind: "witness", value: run.outcome }) as Witness).digest !== source.digest) invalid();
  const decisionWitness = (yield { kind: "witness", value: decision }) as Witness;
  if (request.encoding === "core-completion-ref-v1") {
    const sources = request.sources as Record<string, Json>, expected = sources?.decision as Record<string, Json>;
    if (request.decision !== null || !expected || expected.digest !== decisionWitness.digest || expected.length !== decisionWitness.length) invalid();
  } else if (((yield { kind: "witness", value: request.decision }) as Witness).digest !== decisionWitness.digest) invalid();
  return copyJson(decision.outcome);
}
