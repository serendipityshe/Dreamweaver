import { fixedActionRequestMatches } from "./fixed-action-request.js";
import { encodeActionRequests, expandActionRequests, readActionAcceptance } from "./action-request.js";
import { createHash } from "node:crypto";
import { canonicalJson, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, jsonEqual, parseJson, type ActionRequest, type ActionView, type Json, type PreparedActionRequest } from "../contract/index.js";
import { readProjectedJson, readProjectedJsonSync, reportProjectionWrites, type ReportProjectionBinding } from "./projection.js";
import { actionReportValueReference, reportOperationID, verifyInlineReportReceipt } from "./report-value.js";
import { readActionControl, actionControlDigest } from "./action-control.js";
import { ValueStore } from "./value-store.js";
import type { SQLiteStore } from "./sqlite.js";
import type { SQLWrite } from "./writes.js";
import { readFrameResult } from "./frame-result.js";
import { readCommandProjectionSync } from "./command-value.js";
import { encodeCheckpointFields, expandCheckpointFields, expandCheckpointFieldsAsync } from "./checkpoint-value.js";

function sameJson(leftValue: unknown, rightValue: unknown): boolean {
  const left = canonicalJsonChunks(leftValue)[Symbol.iterator](), right = canonicalJsonChunks(rightValue)[Symbol.iterator]();
  for (;;) {
    const a = left.next(), b = right.next();
    if (a.done || b.done) return a.done === b.done;
    if (!Buffer.from(a.value).equals(Buffer.from(b.value))) return false;
  }
}
function matchesCanonical(value: unknown, source: unknown): boolean {
  return typeof source === "string" && sameJson(value, parseJson(source));
}

/** Mutable snapshots contain state and field identities. Outcomes live in the
 * Action outcome column, observations in their immutable observation record.
 * This format applies equally to running, cancelled and remote Actions. */
export type ActionExecutionState = Omit<ActionView, "outcome" | "latestObservation"> & {
  snapshotFormat: "action-fields-v1";
  outcome: { kind: NonNullable<ActionView["outcome"]>["kind"] } | null;
  latestObservation: { id: string } | null;
};

export function encodeActionSnapshot(action: ActionView | ActionExecutionState): Json {
  action = encodeCheckpointFields(action);
  if ("snapshotFormat" in action) {
    if (action.snapshotFormat !== "action-fields-v1") throw harnessError("INTERNAL_ERROR", "Unknown Action state format");
    return encodeActionRequests(action);
  }
  if (!action.outcome && !action.latestObservation) return encodeActionRequests(action);
  // Representation threshold only, never an admission limit. Small snapshots
  // retain the legacy format, avoiding extra reads for ordinary control work.
  let bytes = 0;
  for (const part of canonicalJsonChunks([action.outcome, action.latestObservation])) {
    bytes += part.byteLength;
    if (bytes > 65_536) break;
  }
  if (bytes <= 65_536) return encodeActionRequests(action);
  return encodeActionRequests({ ...action, snapshotFormat: "action-fields-v1",
    outcome: action.outcome ? { kind: action.outcome.kind } : null,
    latestObservation: action.latestObservation ? { id: action.latestObservation.id } : null });
}

function fieldSnapshot(value: Json): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value) || !Object.hasOwn(value, "snapshotFormat")) return false;
  if (value.snapshotFormat !== "action-fields-v1") throw harnessError("INTERNAL_ERROR", "Unknown Action snapshot format");
  for (const [name, key] of [["outcome", "kind"], ["latestObservation", "id"]] as const) {
    const field = value[name];
    if (field !== null && (!field || typeof field !== "object" || Array.isArray(field) || Object.keys(field).join(",") !== key || typeof field[key] !== "string"))
      throw harnessError("INTERNAL_ERROR", "Invalid Action field identity");
  }
  return true;
}

function observationSource(store: SQLiteStore, row: Record<string, unknown>, template: Json): Record<string, unknown> | null {
  const state = template as unknown as ActionView;
  if (!state.latestObservation) return null;
  const observation = store.query("SELECT * FROM action_observations WHERE id=? AND action_id=?", [state.latestObservation.id, row.id as string])[0];
  if (!observation) throw harnessError("INTERNAL_ERROR", "Action observation source is unavailable");
  return observation;
}

function assembleFields(store: SQLiteStore, row: Record<string, unknown>, template: Json, outcome: Json, observation: Json, source: Record<string, unknown> | null): ActionView {
  const { snapshotFormat: _, ...state } = template as Record<string, Json>;
  const marker = state.outcome as { kind: string } | null;
  if ((marker?.kind ?? null) !== ((outcome as { kind?: string } | null)?.kind ?? null)
      || (source ? !observation || typeof observation !== "object" || Array.isArray(observation)
        || observation.id !== source.id || observation.source !== source.source || observation.attemptID !== source.attempt_id : observation !== null))
    throw harnessError("INTERNAL_ERROR", "Action field source is inconsistent");
  return checkView(store, row, { ...state, outcome, latestObservation: observation });
}

function assertSameSnapshot(store: SQLiteStore, row: Record<string, unknown>): void {
  store.assertAuthority();
  if (!store.query("SELECT 1 AS ok FROM actions WHERE id=? AND run_id=? AND revision=? AND snapshot_json IS ? AND snapshot_projection_id IS ?",
    [row.id as string, row.run_id as string, row.revision as number, row.snapshot_json as string, row.snapshot_projection_id as string | null])[0])
    throw harnessError("STATE_CONFLICT", "Action snapshot changed during reconstruction");
}

/** Dispatch/checkpoint/claim need state, never an observation payload. The
 * separate type prevents a compact field identity from escaping as a view. */
export function readActionExecutionState(store: SQLiteStore, row: Record<string, unknown>): ActionView | ActionExecutionState {
  requireIdentity(row);
  if (row.snapshot_projection_id !== null) throw harnessError("STATE_CONFLICT", "Projected Action requires its control reader");
  const template = expandCheckpointFields(store, expandActionRequests(store, parseJson(row.snapshot_json as string)));
  if (!fieldSnapshot(template)) return checkView(store, row, template);
  const state = template as unknown as ActionExecutionState;
  const control = readActionControl(store, row.id as string);
  if (state.id !== row.id || state.runID !== row.run_id || state.revision !== row.revision
      || control.revision !== state.revision || control.outcomeKind !== (state.outcome?.kind ?? null)
      || !fixedActionRequestMatches(store, state.id, state.request, row.request_json)) throw harnessError("INTERNAL_ERROR", "Action execution state is inconsistent");
  if (state.latestObservation && !store.query("SELECT id FROM action_observations WHERE id=? AND action_id=?", [state.latestObservation.id, state.id])[0])
    throw harnessError("INTERNAL_ERROR", "Action observation source is unavailable");
  return state;
}

/** Original and prepared requests are bound to the immutable acceptance event. */
export function verifyActionAcceptance(store: SQLiteStore, actionID: string, runID: string, original: unknown, request: unknown): void {
  const events = store.query("SELECT data_json FROM history WHERE subject_id=? AND type='action.accepted'", [actionID]);
  if (events.length !== 1) throw harnessError("INTERNAL_ERROR", "Action acceptance evidence is unavailable");
  const event = readActionAcceptance(store, actionID, events[0]!.data_json);
  if (!event || typeof event !== "object" || Array.isArray(event) || event.actionID !== actionID || event.runID !== runID
      || !Object.hasOwn(event, "original") || !Object.hasOwn(event, "request")
      || !sameJson(event.original, original) || !sameJson(event.request, request))
    throw harnessError("INTERNAL_ERROR", "Action acceptance evidence is inconsistent");
}

/** Compare the actual persisted prepared request, not a digest supplied by a
 * response reader, before disclosing a frozen Action response. */
export function verifyActionFixedRequest(store: SQLiteStore, view: ActionView): void {
  const row = store.query("SELECT action_key,request_json FROM actions WHERE id=?", [view.id])[0];
  if (!row || !view.request || !view.original || view.request.key !== row.action_key || view.original.key !== row.action_key
      || !fixedActionRequestMatches(store, view.id, view.request, row.request_json)) throw harnessError("INTERNAL_ERROR", "Action request evidence is inconsistent");
}

/** Build metadata for the caller's same atomic report.observed transaction.
 * The winning outcome and latest observation may name different reports.
 * This does not itself publish a snapshot or bypass the schema write gate. */
export function reportActionSnapshotProjection(action: ActionView, observationReportID: string, outcomeReportID: string | null, alreadyReferenced = false): {
  id: string; writes: SQLWrite[]; source: { observationReportID: string; outcomeReportID: string | null; observationEncoding: "report-ref-v1" | "raw-report-ref-v1" | "inline-diagnostic-v1" };
} | null {
  const observation = action.latestObservation;
  if (!observation || observation.id !== observationReportID || !["execute", "reconcile"].includes(observation.source)
      || !observationReportID || !action.id || !Number.isSafeInteger(action.revision) || action.revision < 0)
    throw harnessError("STATE_CONFLICT", "Action observation cannot be projected from this report");
  if (action.outcome !== null && (action.outcome.kind !== "succeeded" || !outcomeReportID || action.outcomeObservationID !== outcomeReportID)
      || action.outcome === null && outcomeReportID !== null)
    throw harnessError("STATE_CONFLICT", "Action outcome cannot be projected from this report");
  const diagnostic = observation.result === null && observation.reported && typeof observation.reported === "object" && !Array.isArray(observation.reported)
    && observation.reported.kind === "diagnostic" && observation.reported.phase === observation.source
    && ["DEPENDENCY_CREATE_FAILED", "POLICY_FAILED", "CAPACITY_THROWN", "RESULT_CONTRACT_VIOLATION"].includes(observation.reported.code as string);
  // A valid reconcile report can have no result (retryable/unresolved). Its
  // result slot still binds to the report: projection decoding derives null
  // from the original payload instead of trusting an unbound template slot.
  const raw = observation.result === null && !diagnostic && !(observation.source === "reconcile" && observation.validated);
  if (diagnostic && outcomeReportID === null) return null;
  // New large-input snapshots keep their acceptance source when a report
  // arrives. Legacy report projections retain their existing representation.
  if (!alreadyReferenced) {
    let requestBytes = 0;
    for (const part of canonicalJsonChunks([action.original, action.request])) {
      requestBytes += part.length;
      if (requestBytes > 16_384) return null;
    }
  }
  const template = copyJson({ ...encodeCheckpointFields(action), ...(action.outcome === null ? {} : { outcome: null }),
    latestObservation: diagnostic ? observation : raw ? { ...observation, reported: null } : { ...observation, result: null, evidence: null, measurements: null, reported: null } });
  let metadataBytes = 0;
  for (const part of canonicalJsonChunks(template)) {
    metadataBytes += part.byteLength;
    // This is an internal choice of representation, not an admission limit.
    // Keep the old supported representation until all metadata can be stored
    // by reference; never reject an otherwise legal Action because of it.
    if (metadataBytes > 65_536 && !alreadyReferenced) return null;
  }
  const id = `action-snapshot:${canonicalJsonDigest([action.id, action.revision, observationReportID, outcomeReportID])}`;
  const bindings: ReportProjectionBinding[] = [ ...(diagnostic ? [] : raw ? [
    { reportID: observationReportID, path: ["latestObservation", "reported"], selector: "report" },
  ] as ReportProjectionBinding[] : [
    { reportID: observationReportID, path: ["latestObservation", "result"], selector: "result" },
    { reportID: observationReportID, path: ["latestObservation", "evidence"], selector: "evidence" },
    { reportID: observationReportID, path: ["latestObservation", "measurements"], selector: "measurements" },
    { reportID: observationReportID, path: ["latestObservation", "reported"], selector: "report" },
  ] as ReportProjectionBinding[]),
    ...(outcomeReportID ? [{ reportID: outcomeReportID, path: ["outcome"], selector: "result" as const }] : []),
  ];
  return { id, writes: reportProjectionWrites(id, action.id, template, bindings),
    source: { observationReportID, outcomeReportID, observationEncoding: diagnostic ? "inline-diagnostic-v1" : raw ? "raw-report-ref-v1" : "report-ref-v1" } };
}

/** Verify an inline outcome or the explicit reference to its immutable source.
 * The compact kind marker is never itself returned as a business outcome. */
export function verifyActionOutcome(store: SQLiteStore, row: Record<string, unknown>, view: ActionView): void {
  if (row.outcome_projection_id === undefined) throw harnessError("INTERNAL_ERROR", "Action outcome discriminator is missing");
  if (row.outcome_projection_id === null) {
    if (row.outcome_json === null ? view.outcome !== null : !sameJson(view.outcome, readCommandProjectionSync(store, row.outcome_json, view.id)))
      throw harnessError("INTERNAL_ERROR", "Action inline outcome is inconsistent");
    return;
  }
  const frameSource = typeof row.outcome_projection_id === "string" && store.query("SELECT * FROM frame_result_projections WHERE projection_id=?", [row.outcome_projection_id])[0];
  if (frameSource) {
    if (!view.outcome || row.outcome_json !== canonicalJson({ kind: view.outcome.kind })
        || frameSource.paths_json !== "[[]]" || !sameJson(readFrameResult(store, frameSource, view.id), view.outcome))
      throw harnessError("INTERNAL_ERROR", "Remote outcome reference is inconsistent");
    return;
  }
  if (typeof row.outcome_projection_id !== "string" || !view.outcome || !["succeeded", "failed"].includes(view.outcome.kind)
      || row.outcome_json !== canonicalJson({ kind: view.outcome.kind }))
    throw harnessError("INTERNAL_ERROR", "Action outcome reference is invalid");
  const source = store.query(`SELECT r.*,h.request_json observation_request FROM value_projections p
    JOIN value_projection_bindings b ON b.projection_id=p.id JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
    JOIN history h ON h.operation_id='report.observe:'||r.id AND h.type='report.observed' AND h.subject_id=r.action_id
    WHERE p.id=? AND p.action_id=? AND p.template_json='null' AND p.binding_count=1
      AND b.ordinal=0 AND b.path_json='[]' AND b.selector='result' AND r.phase IN ('execute','reconcile')`, [row.outcome_projection_id, view.id])[0];
  if (!source) throw harnessError("INTERNAL_ERROR", "Action outcome source is unavailable");
  const ref = actionReportValueReference(store, source);
  const receipt = parseJson(source.observation_request as string) as { [key: string]: Json };
  if (!ref || !receipt || typeof receipt !== "object" || Array.isArray(receipt)
      || receipt.version !== "report-observation-ref-v1" || receipt.reportID !== source.id
      || receipt.outcomeProjectionID !== row.outcome_projection_id || typeof receipt.outcomeDigest !== "string"
      || !/^[a-f0-9]{64}$/.test(receipt.outcomeDigest) || !jsonEqual(receipt.reportValue, ref)
      || !jsonEqual(receipt.candidate, { kind: "report-result" })) throw harnessError("INTERNAL_ERROR", "Action outcome evidence is inconsistent");
  const hash = createHash("sha256");
  for (const part of canonicalJsonChunks(view.outcome)) hash.update(part);
  if (hash.digest("hex") !== receipt.outcomeDigest) throw harnessError("INTERNAL_ERROR", "Action outcome does not match its evidence");
}

/**
 * Explicit dual read for Action snapshots. Callers must separately check
 * control state and use their usual revision/lease/cancellation CAS after any
 * await. A projection is not a business value and is never returned as one.
 * Legacy inline views, successful report projections and mutable field
 * snapshots have separate discriminators. No compact field is a public value.
 */
function requireIdentity(row: Record<string, unknown>): void {
  if (!row || typeof row.id !== "string" || typeof row.run_id !== "string" || !Number.isSafeInteger(row.revision)
      || row.snapshot_projection_id === undefined || row.snapshot_json === null || row.snapshot_json === undefined)
    throw harnessError("INTERNAL_ERROR", "Action snapshot identity is unavailable");
}
function checkView(store: SQLiteStore, row: Record<string, unknown>, value: Json): ActionView {
  if (!value || typeof value !== "object" || Array.isArray(value) || value.id !== row.id || value.runID !== row.run_id || value.revision !== row.revision)
    throw harnessError("INTERNAL_ERROR", "Action snapshot identity is inconsistent");
  const view = value as unknown as ActionView;
  if (typeof row.action_key !== "string" || !view.request || !view.original || view.request.key !== row.action_key
      || view.original.key !== row.action_key || !fixedActionRequestMatches(store, view.id, view.request, row.request_json))
    throw harnessError("INTERNAL_ERROR", "Action snapshot request is inconsistent");
  verifyActionOutcome(store, row, view);
  return view;
}

/** Synchronous compatibility boundary. Supports inline/field snapshots;
 * report-snapshot projections still require the asynchronous evidence reader.
 * Hot claim/dispatch/checkpoint paths use readActionExecutionState instead. */
export function readInlineActionSnapshot(store: SQLiteStore, row: Record<string, unknown>, evidence = false): ActionView {
  requireIdentity(row);
  if (row.snapshot_projection_id !== null) throw harnessError("STATE_CONFLICT", "Referenced Action snapshot requires asynchronous reading");
  const template = expandCheckpointFields(store, expandActionRequests(store, parseJson(row.snapshot_json as string)));
  if (!fieldSnapshot(template)) return checkView(store, row, template);
  const source = observationSource(store, row, template);
  const outcome = row.outcome_json === null ? null : readProjectedJsonSync(store, row.outcome_projection_id === null ? row.outcome_json : "null", row.outcome_projection_id, row.id as string, evidence);
  const observation = source ? readProjectedJsonSync(store, source.snapshot_json, source.snapshot_projection_id, row.id as string, evidence) : null;
  return assembleFields(store, row, template, outcome, observation, source);
}

export async function readActionSnapshot(store: SQLiteStore, row: Record<string, unknown>): Promise<ActionView> {
  requireIdentity(row);
  if (row.snapshot_projection_id === null) {
    const template = await expandCheckpointFieldsAsync(store, expandActionRequests(store, parseJson(row.snapshot_json as string)));
    if (!fieldSnapshot(template)) { const view = checkView(store, row, template); assertSameSnapshot(store, row); return view; }
    const source = observationSource(store, row, template);
    const outcome = row.outcome_json === null ? null : await readProjectedJson(store, row.outcome_projection_id === null ? row.outcome_json : "null", row.outcome_projection_id, row.id as string);
    const observation = source ? await readProjectedJson(store, source.snapshot_json, source.snapshot_projection_id, row.id as string) : null;
    const view = assembleFields(store, row, template, outcome, observation, source);
    assertSameSnapshot(store, row);
    return view;
  }
  // A valid projection is not sufficient on its own: the current revision's
  // durable receipt must bind the projection to its original report sources.
  const identity = await readActionReuseIdentity(store, row);
  const value = await expandCheckpointFieldsAsync(store, await readProjectedJson(store, row.snapshot_json, row.snapshot_projection_id, row.id as string));
  const view = checkView(store, row, value);
  verifyActionAcceptance(store, view.id, view.runID, view.original, view.request);
  if (!sameJson(identity.original, view.original) || !sameJson(identity.request, view.request))
    throw harnessError("INTERNAL_ERROR", "Action projection identity changed during reconstruction");
  // Referenced content is read across multiple SQLite calls. Recheck the exact
  // source row and authority after reconstruction; callers still must guard
  // their own subsequent state changes.
  if (row.snapshot_projection_id !== null) {
    store.assertAuthority();
    if (!store.query("SELECT 1 AS ok FROM actions WHERE id=? AND run_id=? AND revision=? AND snapshot_json IS ? AND snapshot_projection_id IS ?",
      [row.id as string, row.run_id as string, row.revision as number, row.snapshot_json as string, row.snapshot_projection_id as string])[0])
      throw harnessError("STATE_CONFLICT", "Action snapshot changed during reconstruction");
  }
  return view;
}

/** A command freezes its own revision and observation identity. The Action's
 * final outcome is immutable; an originally empty outcome stays empty even
 * after the current Action settles. Authorize the command target first. */
export function readActionCommandReceipt(store: SQLiteStore, encoded: unknown, actionID: string): ActionView {
  const value = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded);
  if (!value || typeof value !== "object" || Array.isArray(value) || value.id !== actionID || !Number.isSafeInteger(value.revision))
    throw harnessError("INTERNAL_ERROR", "Action command receipt target is inconsistent");
  const current = store.query("SELECT * FROM actions WHERE id=?", [actionID])[0];
  if (!current || current.run_id !== value.runID) throw harnessError("INTERNAL_ERROR", "Action command receipt source is unavailable");
  return readInlineActionSnapshot(store, { ...current, revision: value.revision, snapshot_json: typeof encoded === "string" ? encoded : canonicalJson(value), snapshot_projection_id: null,
    ...(value.outcome === null ? { outcome_json: null, outcome_projection_id: null } : {}) });
}

/** Read only the original/prepared identity for same-key reuse and fenced
 * recovery. No report-derived value is reconstructed on this path. */
export async function readActionReuseIdentity(store: SQLiteStore, row: Record<string, unknown>, evidence = false): Promise<{ original: ActionRequest; request: PreparedActionRequest }> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assert(); const epoch = store.coordinatorEpoch;
  requireIdentity(row);
  if (row.snapshot_projection_id === null) {
    const template = expandActionRequests(store, parseJson(row.snapshot_json as string));
    if (fieldSnapshot(template)) {
      const view = template as unknown as ActionView;
      if (view.id !== row.id || view.runID !== row.run_id || view.revision !== row.revision
          || !view.original || !view.request || view.original.key !== row.action_key || view.request.key !== row.action_key
          || !fixedActionRequestMatches(store, view.id, view.request, row.request_json)) throw harnessError("INTERNAL_ERROR", "Action field identity is inconsistent");
      verifyActionAcceptance(store, view.id, view.runID, view.original, view.request);
      assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Action identity authority changed");
      return { original: view.original, request: view.request };
    }
    const view = checkView(store, row, template);
    assert(); if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Action identity authority changed");
    return { original: view.original, request: view.request };
  }
  if (row.snapshot_json !== "null" || typeof row.snapshot_projection_id !== "string")
    throw harnessError("INTERNAL_ERROR", "Action projection identity is invalid");
  const control = readActionControl(store, row.id as string);
  const projection = store.query("SELECT * FROM value_projections WHERE id=? AND action_id=?", [row.snapshot_projection_id, row.id as string])[0];
  const receiptRow = store.query(`SELECT h.request_json FROM action_controls c JOIN history h ON h.operation_id=c.operation_id
    WHERE c.action_id=? AND c.revision=? AND h.type='report.observed' AND h.subject_id=c.action_id`, [row.id as string, row.revision as number])[0];
  if (!projection || projection.encoding !== "report-projection-v1" || !receiptRow) throw harnessError("INTERNAL_ERROR", "Action projection evidence is unavailable");
  const template = parseJson(projection.template_json as string) as unknown as ActionView;
  const receipt = parseJson(receiptRow.request_json as string) as Record<string, Json>;
  const source = receipt?.snapshotSource;
  if (!template || typeof template !== "object" || Array.isArray(template) || !source || typeof source !== "object" || Array.isArray(source)
      || receipt.version !== "report-observation-ref-v1" || receipt.snapshotProjectionID !== row.snapshot_projection_id
      || receipt.reportID !== source.observationReportID
      || receipt.actionControlDigest !== actionControlDigest(control) || !sameJson(receipt.actionControl, control) || typeof source.observationReportID !== "string"
      || source.outcomeReportID !== null && typeof source.outcomeReportID !== "string"
      || !["report-ref-v1", "raw-report-ref-v1", "inline-diagnostic-v1"].includes(source.observationEncoding as string)
      || template.id !== control.id || template.runID !== control.runID || template.revision !== control.revision || template.status !== control.status
      || template.cancelRequested !== control.cancelRequested || template.recheckRequested !== control.recheckRequested
      || template.executionCount !== control.executionCount || template.outcomeObservationID !== control.outcomeObservationID
      || template.settledAt !== control.settledAt || template.updatedAt !== control.updatedAt || template.remoteRunID !== control.remoteRunID
      || (template.blocked?.code ?? null) !== control.blockedCode || !sameJson(template.latestAttempt, control.latestAttempt)
      || !template.request || template.request.kind !== control.kind || !sameJson(template.request.target, control.target)
      || !template.original || template.request.key !== row.action_key || template.original.key !== row.action_key
      || !fixedActionRequestMatches(store, template.id, template.request, row.request_json) || !template.latestObservation || template.latestObservation.id !== source.observationReportID
      || template.outcome !== null || control.outcomeKind !== "succeeded" || control.outcomeObservationID !== source.outcomeReportID)
    throw harnessError("INTERNAL_ERROR", "Action projection source is inconsistent");
  verifyActionAcceptance(store, control.id, control.runID, template.original, template.request);
  const bindings = store.query("SELECT * FROM value_projection_bindings WHERE projection_id=? ORDER BY ordinal", [row.snapshot_projection_id]);
  const expected: ReportProjectionBinding[] = [ ...(source.observationEncoding === "report-ref-v1" ? [
    { reportID: source.observationReportID, path: ["latestObservation", "result"], selector: "result" },
    { reportID: source.observationReportID, path: ["latestObservation", "evidence"], selector: "evidence" },
    { reportID: source.observationReportID, path: ["latestObservation", "measurements"], selector: "measurements" },
    { reportID: source.observationReportID, path: ["latestObservation", "reported"], selector: "report" },
  ] as ReportProjectionBinding[] : source.observationEncoding === "raw-report-ref-v1" ? [
    { reportID: source.observationReportID as string, path: ["latestObservation", "reported"], selector: "report" as const },
  ] : []), { reportID: source.outcomeReportID as string, path: ["outcome"], selector: "result" } ];
  if (projection.binding_count !== expected.length || bindings.length !== expected.length) throw harnessError("INTERNAL_ERROR", "Action projection bindings are incomplete");
  for (let i = 0; i < expected.length; i++) {
    const actual = bindings[i]!, want = expected[i]!;
    if (actual.ordinal !== i || actual.report_id !== want.reportID || actual.selector !== want.selector || !sameJson(parseJson(actual.path_json as string), want.path))
      throw harnessError("INTERNAL_ERROR", "Action projection binding changed");
  }
  const winning = store.query("SELECT b.report_id FROM value_projections p JOIN value_projection_bindings b ON b.projection_id=p.id WHERE p.id=? AND p.action_id=? AND p.binding_count=1 AND b.path_json='[]' AND b.selector='result'", [row.outcome_projection_id as string, row.id as string])[0];
  if (!winning || winning.report_id !== source.outcomeReportID) throw harnessError("INTERNAL_ERROR", "Action winning outcome source changed");
  const sources = new Set([source.outcomeReportID as string, ...(source.observationEncoding !== "inline-diagnostic-v1" ? [source.observationReportID as string] : [])]);
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  for (const reportID of sources) {
    const report = store.query(`SELECT r.*,p.state processing_state FROM action_reports r JOIN report_processing p ON p.report_id=r.id
      WHERE r.id=? AND r.action_id=?`, [reportID, row.id as string])[0];
    const observation = store.query(`SELECT o.*,p.template_json,p.binding_count AS observation_binding_count,p.encoding AS observation_encoding FROM action_observations o LEFT JOIN value_projections p ON p.id=o.snapshot_projection_id AND p.action_id=o.action_id
      WHERE o.id=? AND o.action_id=?`, [reportID, row.id as string])[0];
    if (!report || report.processing_state !== "done" || !observation || observation.source_id !== reportID || observation.attempt_id !== report.attempt_id
        || observation.source !== report.phase || observation.snapshot_json !== "null" || typeof observation.snapshot_projection_id !== "string" || typeof observation.template_json !== "string"
        || observation.observation_encoding !== "report-projection-v1")
      throw harnessError("INTERNAL_ERROR", "Action projection report observation is unavailable");
    const observationTemplate = parseJson(observation.template_json as string) as Record<string, Json>;
    if (!observationTemplate || observationTemplate.id !== reportID || reportID === source.outcomeReportID && observationTemplate.acceptedAsOutcome !== true
        || reportID === source.observationReportID && !sameJson(observationTemplate, template.latestObservation))
      throw harnessError("INTERNAL_ERROR", "Action projection observation source changed");
    const observationBindings = store.query("SELECT * FROM value_projection_bindings WHERE projection_id=? ORDER BY ordinal", [observation.snapshot_projection_id]);
    const fields = reportID === source.observationReportID && source.observationEncoding === "raw-report-ref-v1" ? [["reported", "report"]] as const
      : [["result", "result"], ["evidence", "evidence"], ["measurements", "measurements"], ["reported", "report"]] as const;
    if (observation.observation_binding_count !== fields.length || observationBindings.length !== fields.length || observationBindings.some((item, index) => item.ordinal !== index || item.report_id !== reportID
        || item.selector !== fields[index]![1] || !sameJson(parseJson(item.path_json as string), [fields[index]![0]])))
      throw harnessError("INTERNAL_ERROR", "Action observation projection binding changed");
    const ref = actionReportValueReference(store, report);
    if (!ref) { verifyInlineReportReceipt(store, report); continue; }
    const digest = createHash("sha256"); let bytes = 0;
    for await (const chunk of evidence ? values.readForEvidence(ref.id, ref.owner) : values.read(ref.id, ref.owner)) { digest.update(chunk); bytes += chunk.byteLength; }
    if (bytes !== ref.length || digest.digest("hex") !== ref.digest) throw harnessError("INTERNAL_ERROR", "Action projection report integrity failed");
  }
  if (source.observationEncoding === "inline-diagnostic-v1") {
    const report = store.query(`SELECT r.*,p.state processing_state FROM action_reports r JOIN report_processing p ON p.report_id=r.id
      WHERE r.id=? AND r.action_id=?`, [source.observationReportID as string, row.id as string])[0];
    const observation = store.query("SELECT * FROM action_observations WHERE id=? AND action_id=?", [source.observationReportID as string, row.id as string])[0];
    const reportReceipt = report && typeof report.attempt_id === "string" && typeof report.phase === "string"
      ? store.query("SELECT * FROM history WHERE operation_id=? AND type='action.report_received' AND subject_id=?",
        [reportOperationID(row.id as string, report.attempt_id, report.phase), row.id as string])[0] : undefined;
    const received = reportReceipt && typeof reportReceipt.request_json === "string" ? parseJson(reportReceipt.request_json) : null;
    if (!report || report.report_value_id !== null || !observation || observation.snapshot_projection_id !== null
        || report.processing_state !== "done" || observation.source_id !== source.observationReportID || observation.source !== report.phase
        || observation.attempt_id !== report.attempt_id || template.latestObservation.acceptedAsOutcome !== false || template.latestObservation.result !== null
        || !received || typeof received !== "object" || Array.isArray(received) || received.actionID !== row.id
        || received.attemptID !== report.attempt_id || received.phase !== report.phase || received.assignmentID !== report.assignment_id
        || received.reportID !== undefined && received.reportID !== report.id || !sameJson(received.report, template.latestObservation.reported)
        || !template.latestObservation.reported || typeof template.latestObservation.reported !== "object" || Array.isArray(template.latestObservation.reported)
        || received.reportDigestScope !== undefined && (received.reportDigest !== template.latestObservation.reported.digest || received.reportDigestScope !== "canonical-report-envelope-utf8-v1")
        || !sameJson(parseJson(report.report_json as string), template.latestObservation.reported)
        || !sameJson(parseJson(observation.snapshot_json as string), template.latestObservation))
      throw harnessError("INTERNAL_ERROR", "Action diagnostic observation evidence changed");
  }
  assert();
  if (store.coordinatorEpoch !== epoch || !store.query("SELECT 1 AS ok FROM actions WHERE id=? AND run_id=? AND revision=? AND snapshot_json IS ? AND snapshot_projection_id IS ?",
    [row.id as string, row.run_id as string, row.revision as number, row.snapshot_json as string, row.snapshot_projection_id])[0])
    throw harnessError("EXECUTION_FENCED", "Action identity changed during verification");
  return { original: template.original, request: template.request };
}
