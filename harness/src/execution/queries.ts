import { submitRequestMetadata, verifySubmitInput } from "../persistence/index.js";
import { commandRequestMetadata, commandSource, verifyCommandValue, readCommandProjectionSync, readCommandProjection, commandEvidenceMetadata, verifyCommandEvidence, type CommandEvidenceSource } from "../persistence/index.js";
import { expandActionRequests } from "../persistence/index.js";
import { readFixedActionRequest, readActionAcceptance } from "../persistence/index.js";
import { readRunHistoryAsync, readRunSnapshot, readRunSnapshotAsync, readRunControl, type RunControl } from "../persistence/index.js";
import { createHmac, timingSafeEqual } from "node:crypto";
import {
  canonicalJson, canonicalJsonChunks, harnessError, jsonEqual, parseJson, validateDecision, validateRPCRequest, validateRPCResponse,
  type ActionView, type CommandView, type DomainInfo, type ErrorInfo, type HistoryRecord, type Json, type PublicMethod, type PublicRPCMap,
  type RPCRequest, type RunView,
} from "../contract/index.js";
import { getVersionPointerAsync } from "../evolution/index.js";
import { authorizeDefinition, authorizeOperation, type PrincipalGrant, type RPCHandler } from "../message/index.js";
import { readActionControl, readActionReport, readActionSnapshot, readInlineActionSnapshot, readProjectedJson, verifyActionAcceptance, verifyActionFixedRequest, type ActionControl, type SQLiteStore } from "../persistence/index.js";
import { readCheckpointHistory, expandCheckpointFields, expandCheckpointFieldsAsync } from "../persistence/index.js";
import { readActionCommandReceipt } from "../persistence/index.js";

type Row = ReturnType<SQLiteStore["query"]>[number];
const commandMethods = {
  submit: "client.submit", cancel: "client.requestCancel", resumeRun: "admin.resumeRun",
  recheckAction: "admin.recheckAction", resolveAction: "admin.resolveAction", adoptVersion: "admin.adoptVersion",
} as const;

/** Shared target authorization for queries and coordinator-private command use cases. */
export function createQueryAccess(store: SQLiteStore, limits: Pick<DomainInfo, "maxPageSize" | "maxResponseBytes"> = { maxPageSize: 100, maxResponseBytes: 4_194_304 }): { handle: RPCHandler; authorizeRun(runID: string, principal: PrincipalGrant): RunView; authorizeAction(actionID: string, principal: PrincipalGrant): ActionView;
  authorizeRunControl(runID: string, principal: PrincipalGrant): RunControl;
  authorizeActionControl(actionID: string, principal: PrincipalGrant): ActionControl;
  authorizeFragment(request: RPCRequest, result: unknown, principal: PrincipalGrant): void; authorizeErrorFragment(request: RPCRequest, error: ErrorInfo, principal: PrincipalGrant): void;
  prepareFragmentAuthorization(request: RPCRequest, result: unknown, principal: PrincipalGrant): unknown;
  authorizeFragmentDescriptor(request: RPCRequest, descriptor: unknown, principal: PrincipalGrant): void } {
  function checked<M extends PublicMethod>(method: M, value: unknown): PublicRPCMap[M]["result"] {
    try {
      const response = validateRPCResponse({ protocolVersion: "harness/1", requestID: "persistent-query",
        binding: { domainID: store.domainID, principalID: "coordinator" }, ok: true, result: value }, method);
      if (!response.ok) throw new Error("Invalid stored response");
      return response.result;
    } catch { throw harnessError("INTERNAL_ERROR", "Stored query record is invalid"); }
  }
  function decode(source: unknown): Json {
    try { return parseJson(source as string); }
    catch { throw harnessError("INTERNAL_ERROR", "Stored JSON record is invalid"); }
  }
  function rawRun(id: string): { view: RunControl; principalID: string } | null {
    const row = store.query("SELECT id,principal_id,revision,frame_count,snapshot_json FROM runs WHERE id=?", [id])[0];
    if (!row) return null;
    const view = readRunControl(store, row.snapshot_json);
    if (view.id !== row.id || view.revision !== row.revision || view.frameCount !== row.frame_count
        || typeof row.principal_id !== "string" || !row.principal_id || row.principal_id.trim() !== row.principal_id) invalidRecord();
    return { view, principalID: row.principal_id as string };
  }
  function rawAction(id: string): ActionView | null {
    const row = store.query("SELECT id,run_id,action_key,revision,request_json,outcome_json,outcome_projection_id,snapshot_json,snapshot_projection_id FROM actions WHERE id=?", [id])[0];
    if (!row) return null;
    const view = checked("client.getAction", readInlineActionSnapshot(store, row));
    if (view.id !== row.id || view.runID !== row.run_id || view.request.key !== row.action_key
        || view.original.key !== row.action_key || view.revision !== row.revision) invalidRecord();
    return view;
  }
  function authorizeTree(run: { view: RunControl; principalID: string }, principal: PrincipalGrant): RunControl {
    const root = run.view.rootID === run.view.id ? run : rawRun(run.view.rootID);
    if (!root || root.view.rootID !== root.view.id || root.view.parentActionID !== null) invalidRecord();
    const owner = root!.principalID;
    const ownClient = principal.roles.includes("client") && owner === principal.principalID;
    const managedAdmin = principal.roles.includes("admin") && (principal.managedPrincipalIDs === "*"
      || principal.managedPrincipalIDs?.includes(owner) === true);
    if (!ownClient && !managedAdmin) throw harnessError("FORBIDDEN", "Task principal is outside the authorized scope");
    // Do not let a child self-report another principal's root: validate its durable parent links.
    let current = run;
    const seen = new Set<string>();
    while (true) {
      if (seen.has(current.view.id) || current.principalID !== owner || current.view.rootID !== root!.view.id
          || !jsonEqual(current.view.limits, root!.view.limits) || seen.size > root!.view.limits.maxDepth) invalidRecord();
      seen.add(current.view.id);
      if (current.view.id === root!.view.id) break;
      if (current.view.parentActionID === null) invalidRecord();
      const parentAction = readActionControl(store, current.view.parentActionID!);
      if (!parentAction || parentAction.kind !== "agent" || parentAction.remoteRunID !== current.view.id
          || !jsonEqual(parentAction.target, current.view.agent)) invalidRecord();
      const parent = rawRun(parentAction!.runID);
      if (!parent) invalidRecord();
      current = parent!;
    }
    return root!.view;
  }
  function getRunControl(id: string, principal: PrincipalGrant): RunControl {
    const run = rawRun(id);
    if (!run) throw harnessError("NOT_FOUND", "Run was not found");
    const root = authorizeTree(run, principal);
    authorizeDefinition(principal, "agent", run.view.agent.id, run.view.agent.version);
    const projected = store.cancellationProjection({ kind: "run", id });
    if (!projected) invalidRecord();
    const view = readRunControl(store, projected.snapshotJson);
    if (view.id !== id || view.rootID !== run.view.rootID || view.parentActionID !== run.view.parentActionID
        || !jsonEqual(view.agent, run.view.agent)) invalidRecord();
    const counted = view.id === root.id ? view : { ...view, rootActionCount: root.rootActionCount };
    return counted.outcome || !projected.cancellation ? counted : { ...counted, cancelRequested: true, cancellation: projected.cancellation };
  }
  function getRun(id: string, principal: PrincipalGrant): RunView {
    const control = getRunControl(id, principal);
    const row = store.query("SELECT snapshot_json FROM runs WHERE id=?", [id])[0];
    if (!row) invalidRecord();
    const view = checked("client.getRun", readRunSnapshot(store, row!.snapshot_json));
    return { ...view, rootActionCount: control.rootActionCount, cancelRequested: control.cancelRequested, cancellation: control.cancellation };
  }
  async function getRunValue(id: string, principal: PrincipalGrant, revalidate: () => PrincipalGrant): Promise<RunView> {
    const control = getRunControl(id, principal);
    const row = store.query("SELECT snapshot_json FROM runs WHERE id=?", [id])[0];
    if (!row) invalidRecord();
    const view = checked("client.getRun", await readRunSnapshotAsync(store, row!.snapshot_json));
    const fresh = revalidate();
    if (fresh.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Session principal changed during query");
    const current = getRunControl(id, fresh);
    store.assertAuthority();
    if (current.revision !== control.revision) throw harnessError("TRANSPORT_UNCERTAIN", "Run changed during query", null, "uncertain");
    if (view.id !== id || view.rootID !== control.rootID || view.parentActionID !== control.parentActionID
        || view.revision !== control.revision || current.revision !== control.revision || !jsonEqual(view.agent, current.agent)) invalidRecord();
    store.assertAuthority();
    return { ...view, rootActionCount: current.rootActionCount, cancelRequested: current.cancelRequested, cancellation: current.cancellation };
  }
  function getAction(id: string, principal: PrincipalGrant): ActionView {
    const action = rawAction(id);
    if (!action) throw harnessError("NOT_FOUND", "Action was not found");
    const run = rawRun(action.runID);
    if (!run) invalidRecord();
    authorizeTree(run!, principal);
    authorizeDefinition(principal, action.request.kind, action.request.target.id, action.request.target.version);
    const projected = store.cancellationProjection({ kind: "action", id });
    if (!projected) invalidRecord();
    const view = checked("client.getAction", action);
    if (view.id !== id || view.runID !== action.runID || !jsonEqual(view.request, action.request)) invalidRecord();
    return view.outcome || !projected.cancellation ? view : { ...view, cancelRequested: true, cancellation: projected.cancellation };
  }
  function actionScope(id: string, principal: PrincipalGrant) {
    const storage = store.query("SELECT snapshot_projection_id,json_extract(snapshot_json,'$.snapshotFormat') snapshot_format,json_extract(snapshot_json,'$.checkpoint.valueSource.encoding') checkpoint_source FROM actions WHERE id=?", [id])[0];
    if (!storage) throw harnessError("NOT_FOUND", "Action was not found");
    if (storage.snapshot_projection_id === null && storage.snapshot_format === null && storage.checkpoint_source === null) getAction(id, principal);
    const control = readActionControl(store, id), run = rawRun(control.runID);
    if (!run) invalidRecord();
    authorizeTree(run!, principal);
    authorizeDefinition(principal, control.kind, control.target.id, control.target.version);
    return control;
  }
  /** An authorized full view is reconstructed only for the public value
   * boundary. Recheck grant and immutable source after the asynchronous read. */
  async function getActionValue(id: string, principal: PrincipalGrant, revalidate: () => PrincipalGrant): Promise<ActionView> {
    const control = actionScope(id, principal);
    const row = store.query("SELECT * FROM actions WHERE id=?", [id])[0];
    if (!row) throw harnessError("NOT_FOUND", "Action was not found");
    const view = checked("client.getAction", await readActionSnapshot(store, row));
    const fresh = revalidate();
    if (fresh.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Session principal changed during query");
    const current = actionScope(id, fresh);
    store.assertAuthority();
    if (current.revision !== control.revision) throw harnessError("TRANSPORT_UNCERTAIN", "Action changed during query", null, "uncertain");
    if (view.id !== control.id || view.runID !== control.runID || view.revision !== control.revision
        || current.id !== control.id || current.runID !== control.runID || current.revision !== control.revision
        || view.request.kind !== current.kind || !jsonEqual(view.request.target, current.target)) invalidRecord();
    const cancellation = store.effectiveCancellation({ kind: "action", id });
    store.assertAuthority();
    return view.outcome || !cancellation ? view : { ...view, cancelRequested: true, cancellation };
  }
  function commandRow(key: string, principal: PrincipalGrant): Row | undefined {
    // Admin management scopes intentionally never participate in command lookup.
    return store.query("SELECT * FROM commands WHERE principal_id=? AND request_key=?", [principal.principalID, key])[0];
  }
  function authorizeCommand(row: Row, principal: PrincipalGrant): RPCRequest {
    let original: RPCRequest;
    try {
      if (typeof row.operation !== "string" || !Object.hasOwn(commandMethods, row.operation)) invalidRecord();
      original = validateRPCRequest({ protocolVersion: "harness/1", requestID: "persistent-command",
        binding: { domainID: store.domainID, principalID: principal.principalID },
        method: commandMethods[row.operation as CommandView["operation"]], params: row.operation === "submit" ? submitRequestMetadata(row.request_json) : row.operation === "resolveAction" ? commandRequestMetadata(row.request_json) : row.operation === "resumeRun" || row.operation === "adoptVersion" ? commandEvidenceMetadata(row.request_json) : decode(row.request_json) });
      if (!("requestKey" in original.params) || original.params.requestKey !== row.request_key) invalidRecord();
    } catch { throw harnessError("INTERNAL_ERROR", "Stored command request is invalid"); }
    authorizeTarget(original, principal, row.status !== "applied");
    if (row.operation === "submit") verifySubmitInput(store, principal.principalID, row.request_key as string, row.request_json);
    if (row.operation === "resolveAction") { const source = commandSource(row.request_json); if (source) { if (source.principalID !== principal.principalID) invalidRecord(); verifyCommandValue(store, source); } }
    if (row.operation === "resumeRun" || row.operation === "adoptVersion") {
      const source = (decode(row.request_json) as unknown as { evidenceSource?: CommandEvidenceSource }).evidenceSource;
      if (source) {
        const subjectID = original.method === "admin.resumeRun" ? original.params.runID : original.method === "admin.adoptVersion" ? canonicalJson([original.params.kind, original.params.ref.id]) : invalidRecord();
        if (source.principalID !== principal.principalID || source.operation !== row.operation || source.subjectID !== subjectID) invalidRecord();
        verifyCommandEvidence(store, source);
      }
    }
    if (row.status === "applied" && (row.operation === "cancel" || row.operation === "resumeRun")) {
      const control = readRunControl(store, row.result_json);
      if (!("runID" in original.params) || control.id !== original.params.runID) invalidRecord();
    }
    return original;
  }
  function command(row: Row, principal: PrincipalGrant, restored?: Json): CommandView {
    const original = authorizeCommand(row, principal);
    const result = restored !== undefined ? restored : row.result_json === null ? null : row.status === "applied" && (row.operation === "cancel" || row.operation === "resumeRun")
      ? readRunSnapshot(store, row.result_json) : row.operation === "recheckAction" && row.status === "applied" && "actionID" in original.params ? readActionCommandReceipt(store, row.result_json, original.params.actionID)
        : row.operation === "resolveAction" && row.status === "applied" ? expandCheckpointFields(store, expandActionRequests(store, readCommandProjectionSync(store, row.result_json))) : readCommandProjectionSync(store, row.result_json);
    const view = checked("client.getCommand", { requestKey: row.request_key, operation: row.operation,
      status: row.status, result, error: row.error_json === null ? null : decode(row.error_json), createdAt: row.created_at, updatedAt: row.updated_at });
    if (!view) invalidRecord();
    if (view!.operation === "submit" && view!.status !== "applied") {
      const preparation = store.query("SELECT candidate_json FROM submit_preparations WHERE principal_id=? AND request_key=?", [principal.principalID, view!.requestKey])[0];
      if (preparation) {
        const candidate = decode(preparation.candidate_json) as { id: string; version: string };
        authorizeDefinition(principal, "agent", candidate.id, candidate.version);
      }
    }
    if (view!.operation === "submit" && view!.status === "applied") {
      const receipt = view!.result as { runID: string; agent: { id: string; version: string } };
      authorizeDefinition(principal, "agent", receipt.agent.id, receipt.agent.version);
      const run = getRunControl(receipt.runID, principal);
      if (original.method !== "client.submit" || !jsonEqual(receipt.agent, run.agent)
          || original.params.agent.id !== receipt.agent.id
          || original.params.agent.version !== undefined && original.params.agent.version !== receipt.agent.version) invalidRecord();
    } else if (view!.status === "applied") {
      const result = view!.result as Record<string, Json>;
      const params = original.params;
      if ("runID" in params && result.id !== params.runID || "actionID" in params && result.id !== params.actionID
          || original.method === "admin.adoptVersion" && (result.kind !== original.params.kind
            || result.id !== original.params.ref.id || !jsonEqual(result.ref, original.params.ref))) invalidRecord();
    }
    return view!;
  }
  async function commandValue(row: Row, principal: PrincipalGrant, revalidate: () => PrincipalGrant): Promise<CommandView> {
    authorizeCommand(row, principal);
    let result: Json | undefined;
    if (row.result_json !== null && row.status === "applied") {
      if (row.operation === "cancel" || row.operation === "resumeRun") result = await readRunSnapshotAsync(store, row.result_json) as unknown as Json;
      else if (row.operation === "resolveAction" || row.operation === "adoptVersion") {
        const stored = parseJson(row.result_json as string);
        if (stored && typeof stored === "object" && !Array.isArray(stored) && (stored.encoding === "command-projection-v1" || stored.encoding === "command-evidence-projection-v1")) {
          result = await readCommandProjection(store, row.result_json);
          if (row.operation === "resolveAction") result = await expandCheckpointFieldsAsync(store, expandActionRequests(store, result));
        }
      }
    }
    const fresh = revalidate();
    if (fresh.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Command query principal changed");
    authorizeOperation(fresh, "client.getCommand"); authorizeCommand(row, fresh); store.assertAuthority();
    const current = commandRow(row.request_key as string, fresh);
    if (!current || current.operation !== row.operation || current.request_json !== row.request_json || current.status !== row.status || current.result_json !== row.result_json || current.error_json !== row.error_json) throw harnessError("STATE_CONFLICT", "Command changed during query");
    return command(row, fresh, result);
  }
  function authorizeTarget(request: RPCRequest, principal: PrincipalGrant, allowMissing = false): void {
    const params = request.params;
    if (request.method === "client.submit") {
      authorizeDefinition(principal, "agent", request.params.agent.id, request.params.agent.version);
    } else if (request.method === "client.getVersion" || request.method === "admin.adoptVersion") {
      if (request.method === "admin.adoptVersion" && principal.domainVersion !== true) {
        throw harnessError("FORBIDDEN", "Domain version management is not authorized");
      }
      const ref = request.method === "admin.adoptVersion" ? request.params.ref : { id: request.params.id };
      authorizeDefinition(principal, request.params.kind, ref.id, "version" in ref ? ref.version : undefined);
    } else if ("runID" in params) {
      if (!allowMissing || rawRun(params.runID)) getRunControl(params.runID, principal);
    } else if ("actionID" in params) {
      if (!allowMissing || store.query("SELECT id FROM actions WHERE id=?", [params.actionID])[0]) actionScope(params.actionID, principal);
    } else if (request.method === "client.getHistory") {
      const subject = request.params.subject;
      if (subject.kind === "run") getRunControl(subject.runID, principal);
      else if (subject.kind === "action") actionScope(subject.actionID, principal);
      else authorizeDefinition(principal, subject.definitionKind, subject.id);
    }
  }
  type Cursor = { domain: string; principal: string; method: string; conditions: Json; high: number; position: number };
  const secret = store.query("SELECT cursor_secret FROM domain WHERE singleton=1")[0]?.cursor_secret;
  if (typeof secret !== "string" || !/^[0-9a-f]{64}$/.test(secret)) invalidRecord();
  const key = Buffer.from(secret as string, "hex");
  function sign(cursor: Cursor): string {
    // Conditions and identity are authenticated as associated data, rather
    // than copied into every cursor. Large identifiers do not inflate each page.
    const body = Buffer.from(canonicalJson({ high: cursor.high, position: cursor.position })).toString("base64url");
    return `${body}.${createHmac("sha256", key).update(canonicalJson(cursor)).digest("base64url")}`;
  }
  function readCursor(value: string, method: string, conditions: Json, principal: PrincipalGrant): Cursor {
    try {
      const parts = value.split(".");
      if (parts.length !== 2 || !parts.every(part => /^[A-Za-z0-9_-]+$/.test(part))) throw new Error();
      const body = Buffer.from(parts[0]!, "base64url");
      if (body.toString("base64url") !== parts[0]) throw new Error();
      const position = parseJson(body.toString("utf8")) as unknown as Pick<Cursor, "high" | "position">;
      if (!position || !Number.isSafeInteger(position.high) || position.high < 0
          || !Number.isSafeInteger(position.position) || position.position < 0 || position.position > position.high
          || Object.keys(position).sort().join(",") !== "high,position") throw new Error();
      const cursor: Cursor = { domain: store.domainID, principal: principal.principalID, method, conditions, ...position };
      const expected = createHmac("sha256", key).update(canonicalJson(cursor)).digest();
      const signature = Buffer.from(parts[1]!, "base64url");
      if (signature.length !== expected.length || signature.toString("base64url") !== parts[1] || !timingSafeEqual(signature, expected)) throw new Error();
      return cursor;
    } catch { throw harnessError("INVALID_ARGUMENT", "Invalid or mismatched cursor"); }
  }
  const publicRunEvents = ["run.accepted", "run.frame_created", "run.decision_committed", "run.cancel_requested", "run.completed", "run.blocked", "wait.created", "wait.resolved", "wait.fact", "wait.pending"];
  const publicActionEvents = ["action.accepted", "attempt.started", "checkpoint.committed", "action.observed", "action.settled", "action.blocked", "action.report_conflict", "action.report_limit_unknown", "resolution.applied"];
  function authorizeRunHistoryItem(runID: string, item: HistoryRecord, principal: PrincipalGrant): void {
    const data = item.data && typeof item.data === "object" && !Array.isArray(item.data) ? item.data : null;
    const actionByKey = (key: unknown) => {
      if (typeof key !== "string") invalidRecord();
      const row = store.query("SELECT id FROM actions WHERE run_id=? AND action_key=?", [runID, key as string])[0];
      if (!row || typeof row.id !== "string") invalidRecord();
      const action = actionScope(row!.id as string, principal);
      if (action.runID !== runID) invalidRecord();
      return action;
    };
    const fact = (value: unknown): void => {
      if (!value || typeof value !== "object" || Array.isArray(value)) invalidRecord();
      const entry = value as { actionID?: unknown; actionKey?: unknown };
      const action = actionByKey(entry.actionKey);
      if (action.id !== entry.actionID) invalidRecord();
    };
    const keys = (value: unknown): void => {
      if (!Array.isArray(value)) invalidRecord();
      for (const key of value as unknown[]) actionByKey(key);
    };
    if (item.type === "run.decision_committed") {
      let decision: ReturnType<typeof validateDecision>;
      try { decision = validateDecision(data?.decision); } catch { return invalidRecord(); }
      if (decision.kind === "dispatch") for (const original of decision.actions) {
        authorizeDefinition(principal, original.kind, original.target.id, original.target.version);
        const action = actionByKey(original.key);
        const stored = store.query("SELECT snapshot_projection_id,request_json FROM actions WHERE id=?", [action.id])[0];
        if (!stored || stored.snapshot_projection_id === undefined) invalidRecord();
        if (stored.snapshot_projection_id === null) {
          if (!jsonEqual(getAction(action.id, principal).original, original)) invalidRecord();
        } else verifyActionAcceptance(store, action.id, runID, original, readFixedActionRequest(store, action.id, stored.request_json));
      }
    } else if (item.type === "wait.fact") fact(data?.fact);
    else if (item.type === "wait.pending") actionByKey(data?.actionKey);
    else if (item.type === "wait.created") keys(data?.actionKeys);
    else if (item.type === "wait.resolved") {
      if (data?.facts !== undefined) {
        if (!Array.isArray(data.facts)) invalidRecord();
        for (const entry of data.facts as unknown[]) fact(entry);
      }
      if (data?.pendingActionKeys !== undefined) keys(data.pendingActionKeys);
    }
  }
  async function page(request: Extract<RPCRequest, { method: "client.listActions" | "client.getHistory" }>, principal: PrincipalGrant, fragmentedResponses: boolean, revalidate: () => PrincipalGrant): Promise<unknown> {
    const params = request.params;
    const limit = params.limit ?? Math.min(limits.maxPageSize, 100);
    if (limit > limits.maxPageSize) throw harnessError("INVALID_ARGUMENT", "Page limit exceeds maxPageSize");
    const conditions: Json = request.method === "client.listActions" ? { runID: request.params.runID, limit } : { subject: request.params.subject, limit };
    // Authenticate the cursor before accessing its target, including for administrators.
    const prior = params.cursor === undefined ? null : readCursor(params.cursor, request.method, conditions, principal);
    authorizeTarget(request, principal);
    let subjectID: string;
    let events: string[] = [];
    if (request.method === "client.listActions") subjectID = request.params.runID;
    else {
      const subject = request.params.subject;
      subjectID = subject.kind === "run" ? subject.runID : subject.kind === "action" ? subject.actionID : canonicalJson([subject.definitionKind, subject.id]);
      events = subject.kind === "run" ? publicRunEvents : subject.kind === "action" ? publicActionEvents : ["version.adopted"];
    }
    const predicate = request.method === "client.listActions" ? "run_id=?" : `subject_id=? AND type IN (${events.map(() => "?").join(",")})`;
    const table = request.method === "client.listActions" ? "actions" : "history";
    const sequence = request.method === "client.listActions" ? "creation_sequence" : "sequence";
    const filter = [subjectID, ...events];
    const high = prior?.high ?? (store.query(`SELECT COALESCE(MAX(${sequence}),0) high FROM ${table} WHERE ${predicate}`, filter)[0]!.high as number);
    const position = prior?.position ?? 0;
    const rows = store.query(`SELECT * FROM ${table} WHERE ${predicate} AND ${sequence}>? AND ${sequence}<=? ORDER BY ${sequence} LIMIT ?`, [...filter, position, high, limit + 1]);
    const base: Cursor = { domain: store.domainID, principal: principal.principalID, method: request.method, conditions, high, position };
    const items: (ActionView | HistoryRecord)[] = [];
    function refreshPrincipal() {
      const refreshed = revalidate();
      if (refreshed.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Session principal changed during query");
      authorizeOperation(refreshed, request.method);
      authorizeTarget(request, refreshed);
      if (request.method === "client.getHistory" && request.params.subject.kind === "run") {
        for (const priorItem of items) authorizeRunHistoryItem(request.params.subject.runID, priorItem as HistoryRecord, refreshed);
      }
      principal = refreshed;
    }
    for (let i = 0; i < Math.min(rows.length, limit); i++) {
      const row = rows[i]!;
      let item: ActionView | HistoryRecord;
      if (request.method === "client.listActions") {
        item = await getActionValue(row.id as string, principal, revalidate);
        refreshPrincipal();
      }
      else {
        const subject = request.params.subject;
        let data: Json;
        if (row.data_projection_id === undefined) invalidRecord();
        if (row.data_projection_id !== null) {
          let projectionActionID = subjectID;
          if (subject.kind === "run" && row.type === "wait.fact") {
            const source = store.query("SELECT action_id FROM value_projections WHERE id=?", [row.data_projection_id as string])[0];
            if (!source || typeof source.action_id !== "string") invalidRecord();
            const action = actionScope(source.action_id as string, principal);
            if (action.runID !== subject.runID) invalidRecord();
            projectionActionID = source.action_id as string;
          } else if (subject.kind !== "action" || row.type !== "action.observed" && row.type !== "action.settled") invalidRecord();
          data = await readProjectedJson(store, row.data_json, row.data_projection_id, projectionActionID);
          refreshPrincipal();
          if (subject.kind === "run" && row.type === "wait.fact") {
            const action = actionScope(projectionActionID, principal);
            if (action.runID !== subject.runID) invalidRecord();
          }
        } else {
          if (subject.kind === "run" && row.type === "wait.fact") {
            const stored = parseJson(row.data_json as string);
            if (stored && typeof stored === "object" && !Array.isArray(stored) && stored.encoding === "wait-fact-history-v1") {
              const fact = stored.fact;
              if (!fact || typeof fact !== "object" || Array.isArray(fact) || typeof fact.actionID !== "string") invalidRecord();
              if (actionScope(fact.actionID as string, principal).runID !== subject.runID) invalidRecord();
            }
          }
          data = subject.kind === "run" ? await readCommandProjection(store, await readRunHistoryAsync(store, subjectID, row.type as string, row.data_json)) : row.type === "action.accepted" ? readActionAcceptance(store, subjectID, row.data_json)
            : row.type === "checkpoint.committed" ? await readCheckpointHistory(store, subjectID, row.data_json) : await readCommandProjection(store, row.data_json);
          refreshPrincipal();
        }
        let body = data && typeof data === "object" && !Array.isArray(data) ? data : null;
        // Upgrade existing unknown-limit events from their immutable report
        // association without changing the event identity or settling it.
        if (row.type === "action.report_limit_unknown" && body && typeof body.reportID === "string" && body.reported === undefined) {
          const original = store.query("SELECT id,action_id,attempt_id,phase,report_json,report_value_id FROM action_reports WHERE id=?", [body.reportID])[0];
          if (!original || original.action_id !== subjectID || original.attempt_id !== body.attemptID) invalidRecord();
          const envelope = await readActionReport(store, original!);
          refreshPrincipal();
          if (envelope?.kind !== "report" || envelope.value === undefined) invalidRecord();
          data = { ...body, phase: original!.phase as string, reported: envelope.value! };
          body = data as { [key: string]: Json };
        }
        if (subject.kind === "version") {
          // A definition-level grant may cover only some versions. History
          // includes the adopted Ref and evidence, so recheck the specific Ref
          // for every item rather than letting an id-only grant expose others.
          const pointer = checked("client.getVersion", body?.pointer);
          if (pointer.kind !== subject.definitionKind || pointer.id !== subject.id || !pointer.ref || pointer.ref.id !== subject.id) invalidRecord();
          authorizeDefinition(principal, subject.definitionKind, subject.id, pointer.ref!.version);
        }
        const observation = body?.observation && typeof body.observation === "object" && !Array.isArray(body.observation) ? body.observation : null;
        const attempt = body?.attemptID ?? observation?.attemptID;
        const actionID = subject.kind === "action" ? subject.actionID : null;
        const runID = subject.kind === "run" ? subject.runID : actionID ? actionScope(actionID, principal).runID : null;
        item = { id: row.id as string, sequence: row.sequence as number, at: row.at as number, runID, actionID,
          attemptID: typeof attempt === "string" ? attempt : null, type: row.type as string, data };
      }
      const last = row[sequence] as number;
      const nextCursor = i + 1 < rows.length ? sign({ ...base, position: last }) : null;
      const candidate = { items: [...items, item], nextCursor };
      const envelope = { protocolVersion: "harness/1", requestID: request.requestID, binding: { domainID: store.domainID, principalID: principal.principalID }, ok: true, result: candidate };
      let envelopeBytes = 0;
      for (const part of canonicalJsonChunks(envelope)) { envelopeBytes += part.byteLength; if (envelopeBytes > limits.maxResponseBytes) break; }
      if (envelopeBytes > limits.maxResponseBytes) {
        if (!items.length && fragmentedResponses) {
          if (request.method === "client.getHistory" && request.params.subject.kind === "run") authorizeRunHistoryItem(request.params.subject.runID, item as HistoryRecord, principal);
          return { items: [item], nextCursor };
        }
        if (!items.length) throw harnessError("LIMIT_EXCEEDED", "A single query record exceeds maxResponseBytes");
        return { items, nextCursor: sign({ ...base, position: rows[i - 1]![sequence] as number }) };
      }
      if (request.method === "client.getHistory" && request.params.subject.kind === "run") authorizeRunHistoryItem(request.params.subject.runID, item as HistoryRecord, principal);
      items.push(item);
    }
    return { items, nextCursor: rows.length > items.length ? sign({ ...base, position: rows[items.length - 1]![sequence] as number }) : null };
  }
  const handle: RPCHandler = (request, principal, revalidate, context) => {
    authorizeOperation(principal, request.method);
    switch (request.method) {
      case "client.getRun": return getRunValue(request.params.runID, principal, revalidate ?? (() => principal));
      case "client.getAction": return getActionValue(request.params.actionID, principal, revalidate ?? (() => principal));
      case "client.getCommand": {
        const row = commandRow(request.params.requestKey, principal);
        return row ? commandValue(row, principal, revalidate ?? (() => principal)) : null;
      }
      case "client.getRunByRequestKey": {
        const row = commandRow(request.params.requestKey, principal);
        if (!row) return null;
        if (row.operation !== "submit") throw harnessError("REQUEST_CONFLICT", "Request key belongs to another operation");
        const receipt = command(row, principal);
        return receipt.status === "applied" ? getRunValue((receipt.result as { runID: string }).runID, principal, revalidate ?? (() => principal)) : null;
      }
      case "client.getVersion": {
        authorizeTarget(request, principal);
        const stored = store.query("SELECT version FROM version_pointers WHERE kind=? AND id=?", [request.params.kind, request.params.id])[0];
        if (stored) authorizeDefinition(principal, request.params.kind, request.params.id, stored.version as string);
        return (async () => {
          const pointer = await getVersionPointerAsync(store, request.params);
          const fresh = revalidate?.() ?? principal;
          if (fresh.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Version query principal changed");
          authorizeOperation(fresh, request.method); authorizeTarget(request, fresh);
          if (pointer.ref) authorizeDefinition(fresh, pointer.kind, pointer.id, pointer.ref.version);
          return pointer;
        })();
      }
      case "client.listActions":
      case "client.getHistory": return page(request, principal, context?.fragmentedResponses === true, revalidate ?? (() => principal));
      default:
        authorizeTarget(request, principal);
        throw harnessError("STATE_CONFLICT", "This operation is not available in the T03 query service");
    }
  };
  function authorizeFragment(request: RPCRequest, result: unknown, principal: PrincipalGrant): void {
    authorizeOperation(principal, request.method);
    const originalRun = (value: unknown): void => {
      const frozen = checked("client.getRun", value), current = getRunControl(frozen.id, principal);
      if (frozen.rootID !== current.rootID || frozen.parentActionID !== current.parentActionID || !jsonEqual(frozen.agent, current.agent)) invalidRecord();
      authorizeDefinition(principal, "agent", frozen.agent.id, frozen.agent.version);
    };
    const originalAction = (value: unknown): void => {
      const frozen = checked("client.getAction", value), current = actionScope(frozen.id, principal);
      if (frozen.runID !== current.runID || frozen.request.kind !== current.kind || !jsonEqual(frozen.request.target, current.target)) invalidRecord();
      verifyActionFixedRequest(store, frozen);
      authorizeDefinition(principal, frozen.request.kind, frozen.request.target.id, frozen.request.target.version);
    };
    const originalVersion = (value: unknown): void => {
      const pointer = checked("client.getVersion", value);
      authorizeDefinition(principal, pointer.kind, pointer.id, pointer.ref?.version);
    };
    const originalCommand = (value: unknown): void => {
      const frozen = checked("client.getCommand", value);
      if (!frozen) return;
      const row = commandRow(frozen.requestKey, principal);
      if (!row || row.operation !== frozen.operation) throw harnessError("FORBIDDEN", "Original command is unavailable");
      authorizeCommand(row, principal);
      if (frozen.status !== "applied" || frozen.result === null) return;
      if (frozen.operation === "submit") {
        const receipt = frozen.result as { runID?: Json; agent?: Json };
        if (typeof receipt.runID !== "string") invalidRecord();
        const run = getRunControl(receipt.runID, principal);
        if (!jsonEqual(receipt.agent, run.agent)) invalidRecord();
      } else if (frozen.operation === "cancel" || frozen.operation === "resumeRun") originalRun(frozen.result);
      else if (frozen.operation === "recheckAction" || frozen.operation === "resolveAction") originalAction(frozen.result);
      else if (frozen.operation === "adoptVersion") originalVersion(frozen.result);
    };
    switch (request.method) {
      case "session.getInfo": return;
      case "client.getRun": case "client.requestCancel": case "admin.resumeRun":
        authorizeTarget(request, principal); originalRun(result); return;
      case "client.getAction": case "admin.recheckAction": case "admin.resolveAction":
        authorizeTarget(request, principal); originalAction(result); return;
      case "client.submit": {
        authorizeTarget(request, principal);
        const receipt = checked("client.submit", result), run = getRunControl(receipt.runID, principal);
        if (!jsonEqual(receipt.agent, run.agent)) invalidRecord();
        return;
      }
      case "client.getRunByRequestKey": {
        const row = commandRow(request.params.requestKey, principal);
        if (!row) { if (result !== null) invalidRecord(); return; }
        if (row.operation !== "submit") throw harnessError("FORBIDDEN", "Original command is unavailable");
        authorizeCommand(row, principal);
        if (result !== null) originalRun(result);
        return;
      }
      case "client.getCommand": {
        const row = commandRow(request.params.requestKey, principal);
        if (!row) { if (result !== null) invalidRecord(); return; }
        authorizeCommand(row, principal); originalCommand(result); return;
      }
      case "client.getVersion": case "admin.adoptVersion":
        authorizeTarget(request, principal); originalVersion(result); return;
      case "client.listActions": {
        authorizeTarget(request, principal);
        const page = checked("client.listActions", result);
        for (const action of page.items) {
          if (action.runID !== request.params.runID) invalidRecord();
          originalAction(action);
        }
        return;
      }
      case "client.getHistory": {
        authorizeTarget(request, principal);
        const page = checked("client.getHistory", result), subject = request.params.subject;
        if (subject.kind === "run") for (const item of page.items) authorizeRunHistoryItem(subject.runID, item, principal);
        if (subject.kind === "version") for (const item of page.items) {
          const data = item.data && typeof item.data === "object" && !Array.isArray(item.data) ? item.data : null;
          const pointer = checked("client.getVersion", data?.pointer);
          if (pointer.kind !== subject.definitionKind || pointer.id !== subject.id || !pointer.ref || pointer.ref.id !== pointer.id) invalidRecord();
          originalVersion(pointer);
        }
        return;
      }
    }
  }
  function authorizeErrorFragment(request: RPCRequest, error: ErrorInfo, principal: PrincipalGrant): void {
    authorizeOperation(principal, request.method);
    // Authorization failures do not establish authority to disclose a cached
    // target response; the transport handles them as ordinary HTTP errors.
    if (error.code === "FORBIDDEN" || error.code === "UNAUTHENTICATED") {
      throw harnessError("FORBIDDEN", "Response fragment is unavailable");
    }
    if (request.method === "client.getCommand" || request.method === "client.getRunByRequestKey") {
      const row = commandRow(request.params.requestKey, principal);
      if (row) authorizeCommand(row, principal);
      return;
    }
    if (request.method !== "session.getInfo") authorizeTarget(request, principal, true);
    const details = error.details && typeof error.details === "object" && !Array.isArray(error.details) ? error.details : null;
    if (error.code === "VERSION_UNAVAILABLE" && details && typeof details.id === "string" && typeof details.version === "string") {
      if (request.method === "client.submit") authorizeDefinition(principal, "agent", details.id, details.version);
      else if (request.method === "admin.adoptVersion") authorizeDefinition(principal, request.params.kind, details.id, details.version);
    }
  }
  type DependencyRow = { sql: string; values: (string | number | null)[] };
  type ActionFragmentState = { request: RPCRequest; result: unknown; token: string; owner: string; kind: "agent" | "capacity"; targetID: string; targetVersion: string; dependencies: DependencyRow[] | null };
  const fragmentDescriptors = new WeakMap<object, ActionFragmentState>();
  const commandFragmentDescriptors = new WeakMap<object, { request: RPCRequest; principalID: string; requestKey: string; operation: unknown; requestJSON: unknown; resultJSON: unknown; status: unknown }>();
  function storeToken(): string {
    // SQLite's connection-local total_changes detects this coordinator's writes;
    // data_version detects commits from every other connection. The durable
    // epoch distinguishes a reopened coordinator connection from its old one.
    const row = store.query("SELECT total_changes() AS local_changes, data_version AS external_changes, (SELECT coordinator_epoch FROM domain WHERE singleton=1) AS epoch FROM pragma_data_version")[0];
    if (!row || ![row.local_changes, row.external_changes, row.epoch].every(value => typeof value === "number" && Number.isSafeInteger(value))) invalidRecord();
    return `${row!.epoch}:${row!.local_changes}:${row!.external_changes}`;
  }
  function actionDependencies(actionID: string, maxBytes: number): DependencyRow[] | null {
    const dependencies: DependencyRow[] = [], seen = new Set<string>();
    let bytes = 0;
    const capture = (table: "actions" | "runs", id: string): Row | null => {
      const columns = table === "actions"
        ? ["id", "run_id", "action_key", "revision", "request_json", "outcome_json", "outcome_projection_id", "snapshot_json", "snapshot_projection_id", "business_cancelled", "business_terminal"]
        : ["id", "principal_id", "revision", "frame_count", "snapshot_json", "parent_action_id", "business_cancelled", "business_terminal"];
      const textColumns = table === "actions" ? ["id", "run_id", "action_key", "request_json", "outcome_json", "outcome_projection_id", "snapshot_json", "snapshot_projection_id"]
        : ["id", "principal_id", "snapshot_json", "parent_action_id"];
      const size = textColumns.map(column => `coalesce(length(CAST(${column} AS BLOB)),0)`).join("+");
      const remaining = maxBytes - bytes;
      const estimate = store.query(`SELECT ${size} AS bytes FROM ${table} WHERE id=?`, [id])[0];
      if (!estimate || typeof estimate.bytes !== "number" || !Number.isSafeInteger(estimate.bytes) || estimate.bytes < 0) invalidRecord();
      if (estimate!.bytes > remaining) return null;
      // Recheck under this SELECT's own SQLite view: another connection can
      // change a row after the size query and before its data is fetched.
      const row = store.query(`SELECT ${columns.join(",")} FROM ${table} WHERE id=? AND ${size}<=?`, [id, remaining])[0];
      if (!row) return null;
      const values = columns.map(column => row![column]);
      if (values.some(value => value !== null && typeof value !== "string" && typeof value !== "number")) invalidRecord();
      for (const value of values) if (typeof value === "string") bytes += Buffer.byteLength(value);
      if (bytes > maxBytes) return null;
      dependencies.push({ sql: `SELECT 1 AS ok FROM ${table} WHERE ${columns.map(column => `${column} IS ?`).join(" AND ")}`, values: values as (string | number | null)[] });
      return row!;
    };
    const action = capture("actions", actionID);
    if (!action) return null;
    let runID = action.run_id as string;
    for (;;) {
      if (seen.has(runID)) invalidRecord();
      seen.add(runID);
      const run = capture("runs", runID);
      if (!run) return null;
      if (run.parent_action_id === null) break;
      if (typeof run.parent_action_id !== "string") invalidRecord();
      const parent = capture("actions", run.parent_action_id as string);
      if (!parent) return null;
      runID = parent.run_id as string;
    }
    // Bound extra retained SQL strings in terms of the already-budgeted
    // response snapshot. Larger ancestry falls back to full authorization.
    return dependencies;
  }
  function actionDependenciesUnchanged(state: ActionFragmentState): boolean {
    if (!state.dependencies || state.request.method !== "client.getAction") return false;
    for (const row of state.dependencies) if (!store.query(row.sql, row.values)[0]) return false;
    // Cancellation validity can change independently of the checked snapshots.
    store.effectiveCancellation({ kind: "action", id: state.request.params.actionID });
    return true;
  }
  function refreshActionFragment(request: RPCRequest, result: unknown, principal: PrincipalGrant): ActionFragmentState {
    if (request.method !== "client.getAction") invalidRecord();
    for (let attempt = 0; attempt < 3; attempt++) {
      const before = storeToken();
      authorizeFragment(request, result, principal);
      const frozen = checked("client.getAction", result);
      if (frozen.id !== request.params.actionID) invalidRecord();
      const run = rawRun(frozen.runID);
      if (!run) invalidRecord();
      const root = rawRun(run!.view.rootID);
      if (!root) invalidRecord();
      let valueBytes = 0;
      for (const part of canonicalJsonChunks(result)) valueBytes += part.byteLength;
      if (!Number.isSafeInteger(valueBytes)) invalidRecord();
      const dependencies = actionDependencies(frozen.id, valueBytes * 2);
      const after = storeToken();
      if (before === after) return { request, result, token: after, owner: root!.principalID,
        kind: frozen.request.kind, targetID: frozen.request.target.id, targetVersion: frozen.request.target.version, dependencies };
    }
    throw harnessError("STATE_CONFLICT", "Task changed during fragment authorization");
  }
  function prepareFragmentAuthorization(request: RPCRequest, result: unknown, principal: PrincipalGrant): unknown {
    if (request.method === "client.getCommand") {
      authorizeFragment(request, result, principal);
      const row = commandRow(request.params.requestKey, principal);
      if (!row) return undefined;
      const descriptor = Object.freeze({});
      commandFragmentDescriptors.set(descriptor, { request, principalID: principal.principalID, requestKey: request.params.requestKey,
        operation: row.operation, requestJSON: row.request_json, resultJSON: row.result_json, status: row.status });
      return descriptor;
    }
    if (request.method !== "client.getAction") return undefined;
    const state = refreshActionFragment(request, result, principal), descriptor = Object.freeze({});
    fragmentDescriptors.set(descriptor, state);
    return descriptor;
  }
  function authorizeFragmentDescriptor(request: RPCRequest, descriptor: unknown, principal: PrincipalGrant): void {
    if (!descriptor || typeof descriptor !== "object") throw harnessError("FORBIDDEN", "Fragment authorization is unavailable");
    const command = commandFragmentDescriptors.get(descriptor);
    if (command) {
      if (command.request !== request || request.method !== "client.getCommand" || command.principalID !== principal.principalID
          || request.binding?.domainID !== store.domainID || request.binding.principalID !== principal.principalID) throw harnessError("FORBIDDEN", "Command fragment authorization is unavailable");
      authorizeOperation(principal, request.method);
      const row = commandRow(command.requestKey, principal);
      if (!row || row.operation !== command.operation || row.request_json !== command.requestJSON || row.result_json !== command.resultJSON || row.status !== command.status) throw harnessError("STATE_CONFLICT", "Command changed during fragment authorization");
      authorizeCommand(row, principal); store.assertAuthority();
      return;
    }
    let state = fragmentDescriptors.get(descriptor);
    if (!state || state.request !== request || request.method !== "client.getAction" || request.binding?.domainID !== store.domainID
        || request.binding.principalID !== principal.principalID) throw harnessError("FORBIDDEN", "Fragment authorization is unavailable");
    for (let attempt = 0; attempt < 3; attempt++) {
      const before = storeToken();
      if (before !== state.token) {
        if (actionDependenciesUnchanged(state) && storeToken() === before) {
          state.token = before;
          continue;
        }
        state = refreshActionFragment(request, state.result, principal);
        fragmentDescriptors.set(descriptor, state);
        continue;
      }
      authorizeOperation(principal, request.method);
      const ownClient = principal.roles.includes("client") && state.owner === principal.principalID;
      const managedAdmin = principal.roles.includes("admin") && (principal.managedPrincipalIDs === "*" || principal.managedPrincipalIDs?.includes(state.owner) === true);
      if (!ownClient && !managedAdmin) throw harnessError("FORBIDDEN", "Task principal is outside the authorized scope");
      authorizeDefinition(principal, state.kind, state.targetID, state.targetVersion);
      if (storeToken() === before) return;
    }
    throw harnessError("STATE_CONFLICT", "Task changed during fragment authorization");
  }
  return { handle, authorizeRun: getRun, authorizeRunControl: getRunControl, authorizeAction: getAction, authorizeActionControl: actionScope, authorizeFragment, authorizeErrorFragment, prepareFragmentAuthorization, authorizeFragmentDescriptor };
}

/** T03 queries share the coordinator-owned connection and never acquire authority. */
export function createQueryHandler(store: SQLiteStore): RPCHandler { return createQueryAccess(store).handle; }

function invalidRecord(): never { throw harnessError("INTERNAL_ERROR", "Stored task relationship is invalid"); }
