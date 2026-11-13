import { encodeRunInput, expectedRunSnapshot } from "../persistence/index.js";
import { fixedActionRequestMatches } from "../persistence/index.js";
import { readFrameDecision } from "../persistence/index.js";
import { readRunSnapshot } from "../persistence/index.js";
import { readProjectedJsonSync, frameResultProjectionWrites, readCompletionRequest, encodeActionSnapshot } from "../persistence/index.js";
import { createHash, randomUUID } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, canonicalJsonDigest, copyJson, parseJson, type ActionObservation, type ActionView, type Fact, type Json, type Outcome, type RunView } from "../contract/index.js";
import { cancellationGuard, readActionSnapshot, readInlineActionSnapshot, readFactOutcome, readFactOutcomeForEvidence, type CommitBatch, type HistoryEvent, type SQLiteStore, type SQLWrite } from "../persistence/index.js";
import { hasWaitResolutionEvents, waitResolutionEvents } from "./wait-history.js";

const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
const equalChunks = (left: unknown, right: unknown): boolean => {
  const a = canonicalJsonChunks(left)[Symbol.iterator](), b = canonicalJsonChunks(right)[Symbol.iterator]();
  for (;;) { const x = a.next(), y = b.next(); if (x.done || y.done) return x.done === y.done; if (!Buffer.from(x.value).equals(Buffer.from(y.value))) return false; }
};

/** Before committing a child result, ensure its parent projection remains readable. */
export function remoteCompletionExceedsResponse(store: SQLiteStore, child: RunView, outcome: Outcome, maxResponseBytes: number): boolean {
  if (child.parentActionID === null) return false;
  const row = store.query("SELECT a.*,r.principal_id FROM actions a JOIN runs r ON r.id=a.run_id WHERE a.id=?", [child.parentActionID])[0];
  if (!row?.snapshot_json || typeof row.principal_id !== "string") throw new Error("Remote parent projection is unavailable");
  const action = readInlineActionSnapshot(store, row), max = Number.MAX_SAFE_INTEGER;
  const observationID = `trusted:${createHash("sha256").update(canonicalJson(["remote", child.id])).digest("hex")}`;
  const sourceEvidence = { childRunID: child.id, operationID: "00000000-0000-0000-0000-000000000000" };
  const observation: ActionObservation = { id: observationID, attemptID: null, source: "remote", observedAt: max, result: outcome,
    evidence: sourceEvidence, validationError: null, measurements: [], reported: json({ payload: outcome, evidence: sourceEvidence, measurements: [] }), validated: true, acceptedAsOutcome: true };
  const next: ActionView = { ...action, revision: max, updatedAt: max, latestObservation: observation, status: "settled", outcome: action.outcome ?? outcome, outcomeObservationID: observationID, settledAt: max, blocked: null };
  const cursor = Buffer.from(canonicalJson({ high: max, position: max })).toString("base64url") + "." + "A".repeat(43);
  const event = { id: "00000000-0000-0000-0000-000000000000", sequence: max, at: max, runID: action.runID, actionID: action.id,
    attemptID: null, type: "action.observed", data: { actionID: action.id, observation } };
  const envelope = (value: unknown) => ({ protocolVersion: "harness/1", requestID: "00000000-0000-0000-0000-000000000000", binding: { domainID: store.domainID, principalID: row.principal_id }, ok: true, result: value });
  return [envelope(next), envelope({ items: [next], nextCursor: cursor }), envelope({ items: [event], nextCursor: cursor })]
    .some(value => Buffer.byteLength(canonicalJson(value), "utf8") > maxResponseBytes);
}

/** Build the parent half of the same transaction that commits the child Core
 * completion. The caller must include its own Core, frame, authorization and
 * fixed-output-validation guards in that transaction. */
export async function prepareRemoteCompletion(store: SQLiteStore, previous: RunView, child: RunView, operationID: string, at: number, maxResponseBytes?: number): Promise<{
  writes: SQLWrite[]; events: HistoryEvent[]; factSnapshot: NonNullable<CommitBatch["factSnapshot"]>; evidence: Json;
} | null> {
  if (child.parentActionID === null) return null;
  const row = store.query("SELECT * FROM actions WHERE id=?", [child.parentActionID])[0];
  const childRow = store.query("SELECT * FROM runs WHERE id=?", [child.id])[0];
  const link = store.query("SELECT * FROM remote_run_links WHERE action_id=? AND child_run_id=?", [child.parentActionID, child.id])[0];
  if (!row?.snapshot_json || !childRow || !link || childRow.snapshot_json !== expectedRunSnapshot(store, previous)) throw new Error("Remote relationship is unavailable");
  if (row.snapshot_projection_id !== null) throw new Error("Referenced remote Action writes are not yet available");
  const action = await readActionSnapshot(store, row);
  const parentRow = store.query("SELECT * FROM runs WHERE id=?", [action.runID])[0];
  if (!parentRow?.snapshot_json) throw new Error("Remote parent is unavailable");
  const parent = readRunSnapshot(store, parentRow.snapshot_json);
  if (action.id !== child.parentActionID || action.request.kind !== "agent" || action.remoteRunID !== child.id || child.id !== previous.id
      || row.run_id !== action.runID || row.revision !== action.revision || row.action_key !== action.request.key || !fixedActionRequestMatches(store, action.id, action.request, row.request_json)
      || parent.id !== action.runID || parentRow.revision !== parent.revision || childRow.revision !== previous.revision || childRow.parent_action_id !== child.parentActionID
      || child.rootID !== parent.rootID || childRow.principal_id !== parentRow.principal_id || canonicalJson(child.agent) !== canonicalJson(action.request.target)
      || canonicalJson(child.input) !== canonicalJson(action.request.input) || canonicalJson(child.config) !== canonicalJson(action.request.config)
      || canonicalJson(child.limits) !== canonicalJson(parent.limits) || child.lifecycle !== "completed" || child.outcome === null
      || child.parentActionID !== previous.parentActionID || child.rootID !== previous.rootID || canonicalJson(child.agent) !== canonicalJson(previous.agent)
      || canonicalJson(child.input) !== canonicalJson(previous.input) || canonicalJson(child.config) !== canonicalJson(previous.config)
      || canonicalJson(child.limits) !== canonicalJson(previous.limits) || previous.outcome !== null || previous.lifecycle === "completed"
      || action.latestAttempt !== null || action.executionCount !== 0 || action.recheckRequested || canonicalJson(action.checkpoint) !== canonicalJson({ revision: 0, value: null })) throw new Error("Remote relationship or fixed request does not match");
  const cancelled = store.effectiveCancellation({ kind: "action", id: action.id });
  const accepted = !action.outcome && !action.cancelRequested && !parent.outcome && !parent.cancelRequested && !cancelled;
  const observationID = `trusted:${createHash("sha256").update(canonicalJson(["remote", child.id])).digest("hex")}`;
  const sourceEvidence = { childRunID: child.id, operationID };
  const observation: ActionObservation = { id: observationID, attemptID: null, source: "remote", observedAt: at, result: child.outcome,
    evidence: sourceEvidence, validationError: null, measurements: [], reported: json({ payload: child.outcome, evidence: sourceEvidence, measurements: [] }), validated: true, acceptedAsOutcome: accepted };
  const next: ActionView = { ...action, revision: action.revision + 1, updatedAt: at, latestObservation: observation,
    ...(accepted ? { status: "settled", outcome: child.outcome, outcomeObservationID: observationID, settledAt: at, blocked: null } : {}) };
  let resultBytes = 0;
  for (const part of canonicalJsonChunks({ observation, outcome: child.outcome })) { resultBytes += part.length; if (resultBytes > 65_536) break; }
  const candidate = resultBytes > 65_536 && store.query("SELECT f.id,c.decision_json FROM frames f JOIN frame_candidates c ON c.frame_id=f.id WHERE f.run_id=? AND f.state='open'", [child.id])[0];
  const decision = candidate ? readFrameDecision(store, candidate.id as string, candidate.decision_json) : null;
  if (candidate && decision?.kind === "complete" && equalChunks(decision.outcome, child.outcome)) {
    const source = { runID: child.id, frameID: candidate.id as string, operationID, digest: canonicalJsonDigest(child.outcome) };
    const observationProjection = `remote-observation:${observationID}`, outcomeProjection = `remote-outcome:${observationID}`;
    const observedProjection = `remote-observed:${observationID}`, settledProjection = `remote-settled:${observationID}`;
    const template = json({ ...observation, result: null, reported: { payload: null, evidence: sourceEvidence, measurements: [] } });
    const factID = accepted ? randomUUID() : null;
    const writes: SQLWrite[] = [
      write("UPDATE remote_run_links SET child_run_id=? WHERE action_id=? AND child_run_id=?", [child.id, action.id, child.id]),
      ...(accepted ? [cancellationGuard({ kind: "action", id: action.id })] : []),
      ...frameResultProjectionWrites(observationProjection, action.id, template, [["result"], ["reported", "payload"]], source),
      ...frameResultProjectionWrites(observedProjection, action.id, json({ actionID: action.id, observation: template }), [["observation", "result"], ["observation", "reported", "payload"]], source),
      ...(accepted ? frameResultProjectionWrites(outcomeProjection, action.id, null, [[]], source) : []),
      write("UPDATE actions SET revision=?,snapshot_json=?,outcome_json=?,outcome_projection_id=? WHERE id=? AND revision=? AND request_json=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS ? AND outcome_projection_id IS ?", [next.revision, { json: encodeActionSnapshot(next) }, accepted ? { json: { kind: child.outcome.kind } } : row.outcome_json as string | null,
        accepted ? outcomeProjection : row.outcome_projection_id as string | null, action.id, action.revision, row.request_json as string, row.snapshot_json as string, row.outcome_json as string | null, row.outcome_projection_id as string | null]),
      write("INSERT INTO action_observations(id,action_id,attempt_id,source,source_id,snapshot_json,snapshot_projection_id) VALUES(?,?,?,?,?,?,?)", [observationID, action.id, null, "remote", child.id, { json: null }, observationProjection]),
      write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [parent.id, parent.revision, parentRow.snapshot_json as string]),
    ];
    const events: HistoryEvent[] = [{ type: "action.observed", subjectID: action.id, data: null, dataProjectionID: observedProjection }];
    if (accepted) {
      writes.push(write("INSERT INTO facts(id,run_id,action_id,outcome_json,outcome_projection_id) VALUES(?,?,?,?,?)", [factID, parent.id, action.id, { json: null }, outcomeProjection]),
        ...frameResultProjectionWrites(settledProjection, action.id, json({ actionID: action.id, outcome: null, sourceObservationID: observationID, settledAt: at }), [["outcome"]], source));
      events.push({ type: "action.settled", subjectID: action.id, data: null, dataProjectionID: settledProjection });
    }
    // The child completion and parent fact remain atomic. Run scheduling uses
    // its existing factSnapshot CAS to resolve the wait without an inline wake.
    return { writes, events, factSnapshot: [], evidence: json({ actionID: action.id, childRunID: child.id, observation, factID, accepted, outcome: child.outcome, resolvedWake: null }) };
  }
  const writes: SQLWrite[] = [write("UPDATE remote_run_links SET child_run_id=? WHERE action_id=? AND child_run_id=?", [child.id, action.id, child.id]),
    ...(accepted ? [cancellationGuard({ kind: "action", id: action.id })] : []),
    write("UPDATE actions SET revision=?,snapshot_json=?,outcome_json=? WHERE id=? AND revision=? AND request_json=? AND snapshot_json=? AND snapshot_projection_id IS NULL AND outcome_json IS ? AND outcome_projection_id IS ?", [next.revision, { json: encodeActionSnapshot(next) }, row.outcome_projection_id != null ? row.outcome_json as string : next.outcome ? { json: json(next.outcome) } : null, action.id, action.revision, row.request_json as string, row.snapshot_json as string, row.outcome_json as string | null, row.outcome_projection_id as string | null]),
    write("INSERT INTO action_observations(id,action_id,attempt_id,source,source_id,snapshot_json) VALUES(?,?,?,?,?,?)", [observationID, action.id, null, "remote", child.id, { json: json(observation) }])];
  const events: HistoryEvent[] = [{ type: "action.observed", subjectID: action.id, data: json({ actionID: action.id, observation }) }];
  const factSnapshot: NonNullable<CommitBatch["factSnapshot"]> = [];
  let factID: string | null = null;
  let resolvedWake: Json = null;
  if (accepted) {
    factID = randomUUID();
    writes.push(write("INSERT INTO facts(id,run_id,action_id,outcome_json) VALUES(?,?,?,?)", [factID, parent.id, action.id, { json: json(child.outcome) }]));
    events.push({ type: "action.settled", subjectID: action.id, data: json({ actionID: action.id, outcome: child.outcome, sourceObservationID: observationID, settledAt: at }) });
    if (parent.wait && parent.wait.winner === null && parent.lifecycle === "suspended") {
      const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=?", [parent.wait.id, parent.id])[0];
      const work = store.query("SELECT id,stage FROM work_items WHERE kind='run' AND subject_id=?", [parent.id])[0];
      if (!wait || wait.winner !== null || wait.action_keys_json !== canonicalJson(parent.wait.actionKeys) || work?.stage !== "waiting") throw new Error("Remote parent wait is inconsistent");
      const facts: (Fact | null)[] = [];
      for (const key of parent.wait.actionKeys) {
        const item = store.query("SELECT id FROM actions WHERE run_id=? AND action_key=?", [parent.id, key])[0];
        if (!item) throw new Error("Remote parent wait action is unavailable");
        const existing = store.query("SELECT id,action_id,outcome_json,outcome_projection_id FROM facts WHERE action_id=?", [item.id as string])[0];
        factSnapshot.push({ actionID: item.id as string, factID: existing?.id as string ?? null });
        facts.push(existing ? { id: existing.id as string, actionID: item.id as string, actionKey: key, outcome: await readFactOutcome(store, existing) }
          : item.id === action.id ? { id: factID, actionID: action.id, actionKey: key, outcome: child.outcome } : null);
      }
      if (facts.every(Boolean)) {
        const wake = { kind: "results", waitID: parent.wait.id, facts: facts as Fact[] };
        resolvedWake = json(wake);
        const updated: RunView = { ...parent, wait: { ...parent.wait, winner: "results" }, revision: parent.revision + 1, updatedAt: at };
        writes.push(write("UPDATE waits SET winner=?,wake_json=? WHERE id=? AND run_id=? AND winner IS NULL AND action_keys_json=?", ["results", { json: json(wake) }, parent.wait.id, parent.id, wait.action_keys_json as string]),
          write("UPDATE runs SET revision=?,snapshot_json=? WHERE id=? AND revision=? AND snapshot_json=?", [updated.revision, { json: encodeRunInput(store, updated) }, parent.id, parent.revision, parentRow.snapshot_json as string]),
          write("UPDATE work_items SET stage=?,result_json=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", ["wake", null, work.id as string, "run", parent.id, "waiting"]));
        events.push(...waitResolutionEvents(store, parent.id, parent.wait.id, facts as Fact[], maxResponseBytes));
      } else writes.push(write("UPDATE waits SET winner=NULL WHERE id=? AND run_id=? AND winner IS NULL AND action_keys_json=?", [parent.wait.id, parent.id, wait.action_keys_json as string]),
        write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [parent.id, parent.revision, parentRow.snapshot_json as string]));
    } else writes.push(write("UPDATE runs SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=?", [parent.id, parent.revision, parentRow.snapshot_json as string]));
  }
  return { writes, events, factSnapshot, evidence: json({ actionID: action.id, childRunID: child.id, observation, factID, accepted, outcome: child.outcome, resolvedWake }) };
}

/** Verify the complete parent half using the original Core operation request.
 * Immutable observation/fact/wait records and events bind it to that request;
 * current Action state may legitimately have advanced after the transaction. */
export function verifyRemoteCompletion(store: SQLiteStore, child: RunView, operationID: string, request: Json): boolean {
  return verifyRemoteCompletionWithFacts(store, child, operationID, request);
}

function verifyRemoteCompletionWithFacts(store: SQLiteStore, child: RunView, operationID: string, request: Json, referencedFacts?: ReadonlyMap<string, Outcome>, evidence = false): boolean {
  try {
    const body = readCompletionRequest<{ remote?: { actionID: string; childRunID: string; observation: ActionObservation; factID: string | null; accepted: boolean; outcome: Json; resolvedWake: null | { kind: string; waitID: string; facts: Fact[] } } | null }>(store, child.id, canonicalJson(request));
    if (child.parentActionID === null) return body.remote == null;
    const remote = body.remote;
    if (!remote || remote.childRunID !== child.id || remote.actionID !== child.parentActionID || !equalChunks(remote.outcome, child.outcome)) return false;
    const proof = store.query("SELECT request_json FROM history WHERE operation_id=? AND subject_id=? AND type='core.complete'", [operationID, child.id])[0];
    if (!proof || proof.request_json !== canonicalJson(request)) return false;
    const link = store.query("SELECT child_run_id FROM remote_run_links WHERE action_id=?", [remote.actionID])[0];
    const row = store.query("SELECT * FROM actions WHERE id=?", [remote.actionID])[0];
    const childRow = store.query("SELECT * FROM runs WHERE id=?", [child.id])[0];
    const parentRow = row && store.query("SELECT * FROM runs WHERE id=?", [row.run_id as string])[0];
    if (!row?.snapshot_json || !childRow || !parentRow?.snapshot_json || link?.child_run_id !== child.id || childRow.principal_id !== parentRow.principal_id) return false;
    const action = readInlineActionSnapshot(store, row, evidence), parent = readRunSnapshot(store, parentRow.snapshot_json);
    if (action.id !== remote.actionID || action.runID !== parent.id || action.remoteRunID !== child.id || action.request.kind !== "agent"
        || canonicalJson(action.request.target) !== canonicalJson(child.agent) || canonicalJson(action.request.input) !== canonicalJson(child.input)
        || canonicalJson(action.request.config) !== canonicalJson(child.config) || child.rootID !== parent.rootID) return false;
    const observation = remote.observation;
    const expectedID = `trusted:${createHash("sha256").update(canonicalJson(["remote", child.id])).digest("hex")}`;
    if (!observation || observation.id !== expectedID || observation.source !== "remote" || observation.attemptID !== null
        || observation.acceptedAsOutcome !== remote.accepted || !equalChunks(observation.result, remote.outcome)
        || canonicalJson(observation.evidence) !== canonicalJson({ childRunID: child.id, operationID })) return false;
    const observationRow = store.query("SELECT * FROM action_observations WHERE id=? AND action_id=? AND source='remote' AND source_id=?", [expectedID, action.id, child.id])[0];
    if (!observationRow || !equalChunks(readProjectedJsonSync(store, observationRow.snapshot_json, observationRow.snapshot_projection_id, action.id, evidence), observation)) return false;
    const eventExists = (type: string, subject: string, data: unknown) => {
      const rows = store.query("SELECT data_json,data_projection_id FROM history WHERE type=? AND subject_id=? AND request_json=?", [type, subject, canonicalJson(request)]);
      return rows.length === 1 && equalChunks(readProjectedJsonSync(store, rows[0]!.data_json, rows[0]!.data_projection_id, action.id, evidence), data);
    };
    if (!eventExists("action.observed", action.id, { actionID: action.id, observation })) return false;
    if (remote.accepted) {
      if (!remote.factID || action.outcomeObservationID !== expectedID || !equalChunks(action.outcome, remote.outcome)) return false;
      const fact = store.query("SELECT * FROM facts WHERE id=? AND action_id=? AND run_id=?", [remote.factID, action.id, parent.id])[0];
      if (!fact || !equalChunks(readProjectedJsonSync(store, fact.outcome_json, fact.outcome_projection_id, action.id, evidence), remote.outcome) || !eventExists("action.settled", action.id, { actionID: action.id, outcome: remote.outcome, sourceObservationID: expectedID, settledAt: observation.observedAt })) return false;
    } else if (remote.factID !== null || remote.resolvedWake !== null) return false;
    if (remote.resolvedWake) {
      const wake = remote.resolvedWake;
      const wait = store.query("SELECT * FROM waits WHERE id=? AND run_id=?", [wake.waitID, parent.id])[0];
      if (wake.kind !== "results" || !wait || wait.winner !== "results" || wait.wake_json !== canonicalJson(wake)
          || !hasWaitResolutionEvents(store, parent.id, wake.waitID, wake.facts)) return false;
      for (const fact of wake.facts) {
        const existing = store.query("SELECT f.outcome_json,f.outcome_projection_id,a.action_key FROM facts f JOIN actions a ON a.id=f.action_id WHERE f.id=? AND f.action_id=? AND f.run_id=?", [fact.id, fact.actionID, parent.id])[0];
        if (!existing || existing.action_key !== fact.actionKey) return false;
        if (existing.outcome_projection_id == null ? existing.outcome_json !== canonicalJson(fact.outcome)
          : !referencedFacts?.has(fact.id) || !equalChunks(referencedFacts.get(fact.id), fact.outcome)) return false;
      }
    }
    return true;
  } catch { return false; }
}

/** Reference-bearing fact evidence must be verified outside a write transaction.
 * Frozen Core recovery remains fail-closed if referenced bytes cannot be read. */
export async function verifyRemoteCompletionReferenced(store: SQLiteStore, child: RunView, operationID: string, request: Json): Promise<boolean> {
  try {
    const epoch = store.coordinatorEpoch;
    const remote = (request as { remote?: { resolvedWake?: { facts?: Fact[] } | null } } | null)?.remote;
    const refs = new Map<string, Outcome>();
    for (const fact of remote?.resolvedWake?.facts ?? []) {
      const row = store.query("SELECT id,action_id,outcome_json,outcome_projection_id FROM facts WHERE id=? AND action_id=?", [fact.id, fact.actionID])[0];
      if (!row) return false;
      if (row.outcome_projection_id != null) refs.set(fact.id, await readFactOutcomeForEvidence(store, row));
    }
    store.assertEvidenceReadAuthority();
    const verified = verifyRemoteCompletionWithFacts(store, child, operationID, request, refs, true);
    store.assertEvidenceReadAuthority();
    return store.coordinatorEpoch === epoch && verified;
  } catch { return false; }
}
