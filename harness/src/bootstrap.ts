import { fixedActionValidationMatches, expandActionRequests } from "./persistence/index.js";
import { checkpointValueByteDelta } from "./persistence/index.js";
import { readRunHistory, readRunSnapshot, readRunFrame, readInlineActionSnapshot, readActionExecutionState, reportProjectionByteBound } from "./persistence/index.js";
import { createHash, randomUUID } from "node:crypto";
import { constants as bufferConstants } from "node:buffer";
import { dirname, resolve } from "node:path";
import { commandRequestMetadata, commandSource, verifyCommandValue, readCommandProjectionSync, commandProjectionByteBound, factHistoryByteBound } from "./persistence/index.js";
import type { ConnectOptions, DomainInfo, HarnessSession, RunLimits } from "./contract/index.js";
import { canonicalJson, canonicalJsonChunks, expandDispatchPayload, copyJson, harnessError, parseJson, validateRPCResponse, type ActionView, type ValidationPayload, type Json } from "./contract/index.js";
import { SQLiteStore, ValueStore, ReportTransferBindings, actionReportValueReference, frameJobInput, readFrameInput, readActionControl, resumeDirectDiagnosticRetirements } from "./persistence/index.js";
import { verifyInlineReportReceipt } from "./persistence/index.js";
import { connect, startRPCServer, StaticAuthentication, authorizeDefinition, CanonicalReportStaging, BoundReportTransfer, ReportTransferConcurrency, type RPCServer } from "./message/index.js";
import { createQueryAccess, createSubmitService, createCancelService, createManagementService, createRecheckService, createResolveService, createRunService, createReportService, createVersionService, BusinessLeaseService, ActionService, reportResponseBytes } from "./execution/index.js";
import type { BusinessLeaseGrant } from "./contract/index.js";
import { createHarnessNode, NodeService, nodeServiceOptions, type NodeServiceOptions } from "./capacity/index.js";

/** The only public runtime entry: host deployment assembly below stays private. */
export function connectHarness(options: ConnectOptions): Promise<HarnessSession> { return connect(options, createHarnessNode); }

type PublishedLimits = Pick<DomainInfo, "defaultLimits" | "maxPayloadBytes" | "maxValueBytes" | "maxFrameBytes" | "maxResponseBytes" | "maxPageSize">;
export interface CoordinatorOptions {
  databasePath: string;
  /** Exact model execution Refs approved for explicit duplicate-charge risk. */
  modelRetryTargets?: import("./contract/index.js").Ref[];
  host?: string;
  port?: number;
  authentication: StaticAuthentication;
  limits?: Partial<PublishedLimits>;
  node?: NodeServiceOptions;
  preparation?: { prepareTimeoutMs?: number; retryAfterMs?: number; pollMs?: number };
  maxRunLimits?: Partial<RunLimits>;
  /** Logical raw-byte quota and bounded admission for coordinator content.
   * This does not reserve physical disk space. */
  valueStore?: { maxBytes?: number; maxConcurrentTransfers?: number; maxPendingTransfers?: number };
}

export interface CoordinatorInspection {
  scope: "none" | "runs" | "coordinator";
  nextStep: "none" | "inspect-evidence" | "use-current-coordinator";
  storage: ReturnType<SQLiteStore["inspectRecovery"]>;
  execution: ReturnType<ReturnType<typeof createRunService>["inspectRecovery"]>;
}

/** Internal assembly. The public runtime facade omits test-only options. */
export async function startCoordinator(options: CoordinatorOptions, testOnly?: { enableBoundReportTransfer: true }): Promise<{ endpoint: string; inspectRecovery(): CoordinatorInspection; close(): Promise<void> }> {
  if (!options || typeof options !== "object" || !(options.authentication instanceof StaticAuthentication)
      || typeof options.databasePath !== "string" || !options.databasePath || options.databasePath === ":memory:"
      || options.host !== undefined && (typeof options.host !== "string" || !options.host.trim())
      || options.port !== undefined && (!Number.isSafeInteger(options.port) || options.port < 0 || options.port > 65535)) {
    throw harnessError("INVALID_ARGUMENT", "Invalid coordinator configuration");
  }
  const { databasePath, authentication, host, port } = options;
  const modelRetryTargets = copyJson(options.modelRetryTargets ?? []);
  if (!Array.isArray(modelRetryTargets) || modelRetryTargets.some(ref => !ref || typeof ref !== "object" || Array.isArray(ref) || Object.keys(ref).some(k => !["id", "version"].includes(k)) || typeof ref.id !== "string" || !ref.id.trim() || typeof ref.version !== "string" || !ref.version.trim())) throw harnessError("INVALID_ARGUMENT", "Invalid model retry whitelist");
  const authorizeModelRef = (target: import("./contract/index.js").Ref) => {
    if (!modelRetryTargets.some(ref => (ref as { id: string; version: string }).id === target.id && (ref as { id: string; version: string }).version === target.version)) throw harnessError("FORBIDDEN", "Model retry target is not approved");
  };
  const limits = configuredLimits(options.limits);
  const maxRunLimits = configuredMaxRunLimits(options.maxRunLimits, limits.defaultLimits);
  const nodeOptions = nodeServiceOptions(options.node);
  const preparation = options.preparation ?? {};
  const valueOptions = options.valueStore === undefined ? {} : options.valueStore;
  if (!valueOptions || typeof valueOptions !== "object" || Array.isArray(valueOptions)
      || Object.keys(valueOptions).some(key => !["maxBytes", "maxConcurrentTransfers", "maxPendingTransfers"].includes(key))
      || valueOptions.maxBytes !== undefined && (!Number.isSafeInteger(valueOptions.maxBytes) || valueOptions.maxBytes <= 0)
      || valueOptions.maxConcurrentTransfers !== undefined && (!Number.isSafeInteger(valueOptions.maxConcurrentTransfers) || valueOptions.maxConcurrentTransfers < 1 || valueOptions.maxConcurrentTransfers > 64)
      || valueOptions.maxPendingTransfers !== undefined && (!Number.isSafeInteger(valueOptions.maxPendingTransfers) || valueOptions.maxPendingTransfers < 0 || valueOptions.maxPendingTransfers > 1024))
    throw harnessError("INVALID_ARGUMENT", "Invalid value store configuration");
  const valueQuota = valueOptions.maxBytes ?? Number.MAX_SAFE_INTEGER;
  const transferGate = new ReportTransferConcurrency(valueOptions.maxConcurrentTransfers ?? 2, valueOptions.maxPendingTransfers ?? 64);
  if (!preparation || typeof preparation !== "object" || Array.isArray(preparation)
      || Object.keys(preparation).some(key => !["prepareTimeoutMs", "retryAfterMs", "pollMs"].includes(key))
      || Object.values(preparation).some(value => !Number.isSafeInteger(value) || value <= 0)) {
    throw harnessError("INVALID_ARGUMENT", "Invalid preparation configuration");
  }
  const store = new SQLiteStore(databasePath, { deferAuthority: true, clock: nodeOptions.clock });
  let server: RPCServer | undefined;
  let active = false;
  try {
    // Once published, a response budget remains available to older records.
    // For pre-watermark databases, retain the former default and allow room for
    // existing serialized records until an operator establishes the watermark.
    const responseFloor = async () => {
      const watermark = store.query("SELECT bytes FROM response_limit_watermark WHERE singleton=1")[0]!.bytes as number;
      // The v25 limit belongs to the original Attempt, including Attempts
      // whose executor has lost authority but may still report an observation.
      // NULL is an unknown legacy limit and must never be inferred here.
      let retainedReportLimit = limits.maxValueBytes, attemptCursor = "";
      for (;;) {
        const rows = store.query("SELECT attempt_id,report_value_limit FROM action_attempt_state WHERE attempt_id>? AND report_value_limit IS NOT NULL ORDER BY attempt_id LIMIT 128", [attemptCursor]);
        for (const row of rows) {
          const value = row.report_value_limit;
          if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) throw harnessError("INVALID_ARGUMENT", "Stored Attempt report limit is invalid");
          retainedReportLimit = Math.max(retainedReportLimit, value);
        }
        if (rows.length < 128) break;
        attemptCursor = rows[rows.length - 1]!.attempt_id as string;
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      // Projected Action snapshots are not inline JSON. Verify their bound
      // report receipts and stored chunks in both startup passes, without
      // constructing a complete ActionView merely to determine a byte floor.
      const verifiedValues = new Set<string>();
      const projectedActionBytes = async (row: Record<string, unknown>): Promise<number> => {
        if (typeof row.action_id !== "string" || typeof row.run_id !== "string" || typeof row.snapshot_projection_id !== "string"
            || !Number.isSafeInteger(row.revision) || row.a !== 4) throw harnessError("INVALID_ARGUMENT", "Stored Action projection is invalid");
        const projection = store.query("SELECT encoding,binding_count,template_json FROM value_projections WHERE id=? AND action_id=?", [row.snapshot_projection_id, row.action_id])[0];
        if (!projection || projection.encoding !== "report-projection-v1" || !Number.isSafeInteger(projection.binding_count)
            || Number(projection.binding_count) <= 0 || typeof projection.template_json !== "string") throw harnessError("INVALID_ARGUMENT", "Stored Action projection is unavailable");
        const template = parseJson(projection.template_json);
        if (!template || typeof template !== "object" || Array.isArray(template) || template.id !== row.action_id
            || template.runID !== row.run_id || template.revision !== row.revision) throw harnessError("INVALID_ARGUMENT", "Stored Action projection identity changed");
        const controlRow = store.query("SELECT 1 AS ok FROM action_controls WHERE action_id=? AND revision=?", [row.action_id, row.revision as number])[0];
        if (!controlRow) throw harnessError("INVALID_ARGUMENT", "Stored Action control is unavailable");
        const control = readActionControl(store, row.action_id);
        if (control.runID !== row.run_id || control.revision !== row.revision || template.status !== control.status
            || template.cancelRequested !== control.cancelRequested || template.recheckRequested !== control.recheckRequested
            || template.executionCount !== control.executionCount || template.outcomeObservationID !== control.outcomeObservationID)
          throw harnessError("INVALID_ARGUMENT", "Stored Action control changed");
        const bindings = store.query("SELECT ordinal,report_id,path_json,selector FROM value_projection_bindings WHERE projection_id=? ORDER BY ordinal", [row.snapshot_projection_id]);
        if (bindings.length !== projection.binding_count) throw harnessError("INVALID_ARGUMENT", "Stored Action projection bindings are incomplete");
        let bound = Buffer.byteLength(projection.template_json, "utf8") + checkpointValueByteDelta(store, row.action_id, template.checkpoint!);
        const paths: (string | number)[][] = [];
        for (const [ordinal, binding] of bindings.entries()) {
          if (binding.ordinal !== ordinal || typeof binding.report_id !== "string" || typeof binding.path_json !== "string"
              || !["report", "result", "value", "evidence", "measurements"].includes(binding.selector as string))
            throw harnessError("INVALID_ARGUMENT", "Stored Action projection binding is invalid");
          const path = parseJson(binding.path_json);
          if (!Array.isArray(path) || path.some(part => typeof part === "string" ? !part.length : !Number.isSafeInteger(part) || (part as number) < 0))
            throw harnessError("INVALID_ARGUMENT", "Stored Action projection path is invalid");
          const parts = path as (string | number)[];
          if (paths.some(prior => prior.every((part, index) => parts[index] === part) || parts.every((part, index) => prior[index] === part)))
            throw harnessError("INVALID_ARGUMENT", "Stored Action projection paths overlap");
          paths.push(parts);
          let slot: import("./contract/index.js").Json = template;
          for (const part of parts) {
            if (!slot || typeof slot !== "object" || Array.isArray(slot) !== (typeof part === "number") || !Object.hasOwn(slot, part))
              throw harnessError("INVALID_ARGUMENT", "Stored Action projection path is unavailable");
            slot = (slot as { [key: string]: import("./contract/index.js").Json })[part]!;
          }
          if (slot !== null) throw harnessError("INVALID_ARGUMENT", "Stored Action projection slot is not empty");
          const report: Record<string, unknown> | undefined = store.query("SELECT * FROM action_reports WHERE id=? AND action_id=?", [binding.report_id, row.action_id])[0];
          const ref = report && actionReportValueReference(store, report);
          if (!report || report.phase !== "execute" && report.phase !== "reconcile") throw harnessError("INVALID_ARGUMENT", "Stored Action projection report is unavailable");
          if (!ref) {
            verifyInlineReportReceipt(store, report);
            bound += Buffer.byteLength(report.report_json as string, "utf8");
            if (!Number.isSafeInteger(bound)) throw harnessError("INVALID_ARGUMENT", "Stored Action projection exceeds supported size");
            continue;
          }
          const receipt: Record<string, unknown> | undefined = store.query("SELECT type,subject_id,request_json FROM history WHERE operation_id=?", [`report.observe:${report.id}`])[0];
          const request = receipt && typeof receipt.request_json === "string" ? parseJson(receipt.request_json) : null;
          if (!receipt || receipt.type !== "report.observed" || receipt.subject_id !== row.action_id || !request || typeof request !== "object"
              || Array.isArray(request) || request.version !== "report-observation-ref-v1" || request.reportID !== report.id || !request.reportValue)
            throw harnessError("INVALID_ARGUMENT", "Stored Action projection observation is unavailable");
          const expected = canonicalJsonChunks(ref)[Symbol.iterator](), actual = canonicalJsonChunks(request.reportValue)[Symbol.iterator]();
          for (;;) {
            const left = expected.next(), right = actual.next();
            if (left.done || right.done) { if (left.done !== right.done) throw harnessError("INVALID_ARGUMENT", "Stored Action projection source changed"); break; }
            if (!Buffer.from(left.value).equals(Buffer.from(right.value))) throw harnessError("INVALID_ARGUMENT", "Stored Action projection source changed");
          }
          bound += ref.length;
          if (!Number.isSafeInteger(bound)) throw harnessError("INVALID_ARGUMENT", "Stored Action projection exceeds supported size");
          if (!verifiedValues.has(ref.id)) {
            const digest = createHash("sha256"), utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }); let offset = 0;
            while (offset < ref.length) {
              const chunk = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [ref.id, offset])[0];
              if (!chunk || !(chunk.payload_blob instanceof Uint8Array) || chunk.length_bytes !== chunk.payload_blob.length
                  || !chunk.payload_blob.length || chunk.payload_blob.length > 65_536 || chunk.payload_blob.length > ref.length - offset
                  || createHash("sha256").update(chunk.payload_blob).digest("hex") !== chunk.chunk_digest)
                throw harnessError("INVALID_ARGUMENT", "Stored Action projection content is corrupt");
              try { utf8.decode(chunk.payload_blob, { stream: true }); } catch { throw harnessError("INVALID_ARGUMENT", "Stored Action projection is not UTF-8"); }
              digest.update(chunk.payload_blob); offset += chunk.payload_blob.length;
              await new Promise<void>(resolve => setImmediate(resolve));
            }
            try { utf8.decode(); } catch { throw harnessError("INVALID_ARGUMENT", "Stored Action projection is not UTF-8"); }
            const verified = actionReportValueReference(store, report);
            if (digest.digest("hex") !== ref.digest || !verified || verified.id !== ref.id || verified.owner !== ref.owner
                || verified.encoding !== ref.encoding || verified.length !== ref.length || verified.digest !== ref.digest)
              throw harnessError("INVALID_ARGUMENT", "Stored Action projection digest changed");
            verifiedValues.add(ref.id);
          }
        }
        if (!store.query("SELECT 1 AS ok FROM actions WHERE id=? AND run_id=? AND revision=? AND snapshot_json='null' AND snapshot_projection_id=?",
          [row.action_id, row.run_id, row.revision as number, row.snapshot_projection_id])[0])
          throw harnessError("STATE_CONFLICT", "Stored Action projection changed during startup");
        return bound;
      };
      const fieldActionBytes = async (row: Record<string, unknown>): Promise<number> => {
        const current = store.query(`SELECT id,run_id,revision,action_key,request_json,snapshot_json,snapshot_projection_id,outcome_projection_id,
          length(CAST(outcome_json AS BLOB)) outcome_bytes FROM actions WHERE id=?`, [row.action_id as string])[0];
        if (!current || current.revision !== row.revision || current.snapshot_projection_id !== null) throw harnessError("STATE_CONFLICT", "Stored Action fields changed during startup");
        const state = readActionExecutionState(store, current);
        if (!("snapshotFormat" in state)) throw harnessError("INVALID_ARGUMENT", "Stored Action field encoding changed");
        const storedState = parseJson(current.snapshot_json as string) as Record<string, import("./contract/index.js").Json>;
        let bound = Buffer.byteLength(current.snapshot_json as string, "utf8") + checkpointValueByteDelta(store, state.id, storedState.checkpoint!);
        bound += current.outcome_projection_id === null ? Number(current.outcome_bytes ?? 0)
          : await reportProjectionByteBound(store, current.outcome_projection_id as string, state.id, verifiedValues);
        if (state.latestObservation) {
          const source = store.query("SELECT snapshot_projection_id,length(CAST(snapshot_json AS BLOB)) bytes FROM action_observations WHERE id=? AND action_id=?", [state.latestObservation.id, state.id])[0];
          if (!source) throw harnessError("INVALID_ARGUMENT", "Stored Action observation is unavailable");
          bound += source.snapshot_projection_id === null ? Number(source.bytes)
            : await reportProjectionByteBound(store, source.snapshot_projection_id as string, state.id, verifiedValues);
        }
        if (!Number.isSafeInteger(bound) || !store.query("SELECT id FROM actions WHERE id=? AND revision=? AND snapshot_json=?", [state.id, state.revision, current.snapshot_json as string])[0])
          throw harnessError("STATE_CONFLICT", "Stored Action field size changed during startup");
        return bound;
      };
      // Bound each synchronous read and yield between batches so a startup
      // sharing this process does not monopolize the old coordinator's loop.
      // The second pass runs after fencing, before this coordinator starts work.
      const scans = [
        { kind: "snapshot", sql: "SELECT id AS k,id run_id,json_extract(snapshot_json,'$.snapshotFormat') run_snapshot_format,json_type(snapshot_json,'$.inputSource') run_input_source,json_type(snapshot_json,'$.continuationSource') run_continuation_source,length(CAST(snapshot_json AS BLOB)) a, length(CAST(json_quote(id) AS BLOB)) b, length(CAST(json_quote(principal_id) AS BLOB)) c FROM runs WHERE id>? ORDER BY id LIMIT ?", key: [""], columns: ["k"] },
        { kind: "snapshot", sql: "SELECT creation_sequence AS k, id action_id,run_id,revision,snapshot_projection_id,json_extract(snapshot_json,'$.snapshotFormat') snapshot_format,length(CAST(snapshot_json AS BLOB)) a, length(CAST(json_quote(id) AS BLOB)) b, length(CAST(json_quote(run_id) AS BLOB)) c FROM actions WHERE creation_sequence>? ORDER BY creation_sequence LIMIT ?", key: [0], columns: ["k"] },
        { kind: "history", sql: "SELECT h.sequence AS k,h.subject_id history_subject_id,h.type history_type,json_extract(h.data_json,'$.encoding') history_encoding, length(CAST(h.data_json AS BLOB)) a, length(CAST(json_quote(h.subject_id) AS BLOB)) b, length(CAST(json_quote(a.run_id) AS BLOB)) c, length(CAST(json_quote(json_extract(h.data_json,'$.attemptID')) AS BLOB)) d, length(CAST(json_quote(json_extract(h.data_json,'$.observation.attemptID')) AS BLOB)) e FROM history h LEFT JOIN actions a ON a.id=h.subject_id WHERE h.sequence>? ORDER BY h.sequence LIMIT ?", key: [0], columns: ["k"] },
        { kind: "command", sql: "SELECT principal_id AS k, request_key AS l, json_extract(result_json,'$.snapshotFormat') command_result_format,json_extract(result_json,'$.encoding') command_result_encoding,json_type(result_json,'$.inputSource') command_input_source,json_type(result_json,'$.continuationSource') command_continuation_source, length(CAST(result_json AS BLOB)) a, length(CAST(error_json AS BLOB)) b, length(CAST(json_quote(request_key) AS BLOB)) c, length(CAST(json_quote(principal_id) AS BLOB)) d FROM commands WHERE (principal_id,request_key)>(?,?) ORDER BY principal_id,request_key LIMIT ?", key: ["", ""], columns: ["k", "l"] },
        { kind: "version", sql: "SELECT kind AS k, id AS l,json_extract(evidence_json,'$.encoding') evidence_encoding, length(CAST(evidence_json AS BLOB)) a, length(CAST(json_quote(id) AS BLOB)) b, length(CAST(json_quote(version) AS BLOB)) c FROM version_pointers WHERE (kind,id)>(?,?) ORDER BY kind,id LIMIT ?", key: ["", ""], columns: ["k", "l"] },
        { kind: "cancellation", sql: "SELECT id AS k, length(CAST(cancellation_json AS BLOB)) a FROM cancellation_barriers WHERE id>? ORDER BY id LIMIT ?", key: [""], columns: ["k"] },
      ];
      let largest: number | null = null, largestSnapshot = 0, largestCancellation = 0;
      for (const scan of scans) {
        let key = scan.key as (string | number)[];
        for (;;) {
          const rows = store.query(scan.sql, [...key, 128]);
          for (const row of rows) {
            const size = (field: string) => typeof row[field] === "number" ? row[field] as number : 0;
            let a = row.snapshot_projection_id != null ? await projectedActionBytes(row) : row.snapshot_format != null ? await fieldActionBytes(row) : size("a");
            if (typeof row.action_id === "string" && row.snapshot_projection_id === null && row.snapshot_format === null) {
              const stored = store.query("SELECT snapshot_json FROM actions WHERE id=? AND revision=?", [row.action_id, row.revision as number])[0];
              if (!stored) throw harnessError("STATE_CONFLICT", "Action changed during startup");
              const state = parseJson(stored.snapshot_json as string) as Record<string, import("./contract/index.js").Json>;
              a += checkpointValueByteDelta(store, row.action_id, state.checkpoint!);
            }
            if (row.history_type === "checkpoint.committed") {
              const stored = store.query("SELECT data_json FROM history WHERE sequence=?", [row.k as number])[0];
              if (!stored) throw harnessError("STATE_CONFLICT", "Checkpoint history changed during startup");
              const state = parseJson(stored.data_json as string) as Record<string, import("./contract/index.js").Json>;
              a += checkpointValueByteDelta(store, row.history_subject_id as string, state.checkpoint!);
            }
            if (row.run_snapshot_format != null || row.run_input_source != null || row.run_continuation_source != null) {
              const stored = store.query("SELECT snapshot_json FROM runs WHERE id=?", [row.run_id as string])[0];
              if (!stored) throw harnessError("STATE_CONFLICT", "Run changed during startup");
              a = 0;
              for (const part of canonicalJsonChunks(readRunSnapshot(store, stored.snapshot_json))) {
                a += part.byteLength; await new Promise<void>(resolve => setImmediate(resolve));
              }
              if (!Number.isSafeInteger(a) || !store.query("SELECT id FROM runs WHERE id=? AND snapshot_json=?", [row.run_id as string, stored.snapshot_json as string])[0])
                throw harnessError("STATE_CONFLICT", "Run changed during startup");
            }
            if (row.command_result_format === "run-outcome-ref-v1" || row.command_input_source != null || row.command_continuation_source != null) {
              const stored = store.query("SELECT operation,result_json FROM commands WHERE principal_id=? AND request_key=?", [row.k as string, row.l as string])[0];
              if (!stored || stored.operation !== "cancel" && stored.operation !== "resumeRun") throw harnessError("INVALID_ARGUMENT", "Invalid referenced command result");
              a = 0;
              for (const part of canonicalJsonChunks(readRunSnapshot(store, stored.result_json))) {
                a += part.byteLength; await new Promise<void>(resolve => setImmediate(resolve));
              }
              if (!Number.isSafeInteger(a) || !store.query("SELECT request_key FROM commands WHERE principal_id=? AND request_key=? AND result_json=?", [row.k as string, row.l as string, stored.result_json as string])[0])
                throw harnessError("STATE_CONFLICT", "Command changed during startup");
            }
            if (row.command_result_encoding === "command-evidence-projection-v1" || row.command_result_encoding === "command-projection-v1") {
              const stored = store.query("SELECT result_json FROM commands WHERE principal_id=? AND request_key=?", [row.k as string, row.l as string])[0];
              if (!stored) throw harnessError("STATE_CONFLICT", "Command changed during startup");
              a = store.coordinatorEpoch === 0 ? commandProjectionByteBound(store, stored.result_json) : 0;
              if (store.coordinatorEpoch !== 0) for (const part of canonicalJsonChunks(readCommandProjectionSync(store, stored.result_json))) { a += part.byteLength; await new Promise<void>(resolve => setImmediate(resolve)); }
            }
            if (row.evidence_encoding === "command-evidence-projection-v1") {
              const stored = store.query("SELECT evidence_json FROM version_pointers WHERE kind=? AND id=?", [row.k as string, row.l as string])[0];
              if (!stored) throw harnessError("STATE_CONFLICT", "Version changed during startup");
              a = store.coordinatorEpoch === 0 ? commandProjectionByteBound(store, stored.evidence_json) : 0;
              if (store.coordinatorEpoch !== 0) for (const part of canonicalJsonChunks(readCommandProjectionSync(store, stored.evidence_json))) { a += part.byteLength; await new Promise<void>(resolve => setImmediate(resolve)); }
            }
            if (row.history_encoding === "run-result-history-v1" || row.history_encoding === "run-dispatch-history-v1" || row.history_encoding === "command-evidence-projection-v1" || row.history_encoding === "command-projection-v1") {
              const stored = store.query("SELECT data_json FROM history WHERE sequence=?", [row.k as number])[0];
              if (!stored) throw harnessError("STATE_CONFLICT", "History changed during startup");
              const commandProjection = row.history_encoding === "command-projection-v1" || row.history_encoding === "command-evidence-projection-v1";
              a = store.coordinatorEpoch === 0 && commandProjection ? commandProjectionByteBound(store, stored.data_json) : 0;
              const value = store.coordinatorEpoch === 0 && commandProjection ? undefined : row.history_encoding === "command-projection-v1" ? readCommandProjectionSync(store, stored.data_json) : readRunHistory(store, row.history_subject_id as string, row.history_type as string, stored.data_json);
              if (value !== undefined) for (const part of canonicalJsonChunks(value)) {
                a += part.byteLength; await new Promise<void>(resolve => setImmediate(resolve));
              }
            }
            if (row.history_encoding === "wait-fact-history-v1") {
              const stored = store.query("SELECT data_json FROM history WHERE sequence=?", [row.k as number])[0];
              if (!stored) throw harnessError("STATE_CONFLICT", "Wait fact history changed during startup");
              a = store.coordinatorEpoch === 0 ? factHistoryByteBound(store, row.history_subject_id as string, parseJson(stored.data_json as string)) : 0;
              if (store.coordinatorEpoch !== 0) for (const part of canonicalJsonChunks(readRunHistory(store, row.history_subject_id as string, row.history_type as string, stored.data_json))) { a += part.byteLength; await new Promise<void>(resolve => setImmediate(resolve)); }
            }
            const b = size("b"), c = size("c"), d = size("d"), e = size("e");
            const estimate = scan.kind === "history" ? a + 2 * b + c + d + e + 512
              : scan.kind === "version" ? a + 2 * b + c + 512
              : a + b + c + d + 512;
            if (scan.kind === "snapshot") largestSnapshot = Math.max(largestSnapshot, estimate);
            if (scan.kind === "cancellation") largestCancellation = Math.max(largestCancellation, a);
            largest = Math.max(largest ?? 0, estimate);
          }
          if (rows.length < 128) break;
          const last = rows[rows.length - 1]!;
          key = scan.columns.map(column => last[column] as string | number);
          await new Promise<void>(resolve => setImmediate(resolve));
        }
      }
      // A report received by the previous coordinator may still be awaiting
      // observation. Include its actual public projection, including any
      // already-effective ancestor cancellation, before lowering limits.
      // The scan is repeated after the previous coordinator has been fenced.
      let reportID = "";
      for (;;) {
        const row = store.query(`SELECT p.id,p.action_id,p.attempt_id,p.phase,p.report_json,p.report_value_id,r.principal_id
          FROM action_reports p JOIN actions a ON a.id=p.action_id JOIN runs r ON r.id=a.run_id
          WHERE p.id>? AND NOT EXISTS (SELECT 1 FROM action_observations o WHERE o.id=p.id)
          ORDER BY p.id LIMIT 1`, [reportID])[0];
        if (!row) break;
        reportID = row.id as string;
        // A published referenced report is served by the negotiated chunked
        // path. Its potentially unbounded future projection is not a single
        // physical response budget; verify its source/receipt without reading
        // or parsing its entire content before coordinator activation.
        const referenced = actionReportValueReference(store, row);
        if (referenced) { await new Promise<void>(resolve => setImmediate(resolve)); continue; }
        const actionRow = store.query("SELECT *,id action_id,length(CAST(snapshot_json AS BLOB)) a,json_extract(snapshot_json,'$.snapshotFormat') snapshot_format FROM actions WHERE id=?", [row.action_id as string])[0];
        if (!actionRow?.snapshot_json) throw harnessError("INVALID_ARGUMENT", "Stored report Action is unavailable");
        if (actionRow.snapshot_projection_id !== null || actionRow.snapshot_format === "action-fields-v1") {
          // A late inline report can target a referenced Action. Keep the
          // startup floor conservative without materializing that Action.
          const actionBytes = actionRow.snapshot_projection_id !== null ? await projectedActionBytes(actionRow) : await fieldActionBytes(actionRow);
          let identities = 0;
          for (const part of canonicalJsonChunks([store.domainID, row.principal_id, row.action_id, row.attempt_id])) identities += part.byteLength;
          const withQueryIdentity = actionBytes + 6 * Buffer.byteLength(row.report_json as string, "utf8")
            + 4 * identities + 2 * largestCancellation + limits.maxPayloadBytes + 4096;
          if (!Number.isSafeInteger(withQueryIdentity)) throw harnessError("INVALID_ARGUMENT", "Stored report exceeds the supported response budget");
          largest = Math.max(largest ?? 0, withQueryIdentity);
          await new Promise<void>(resolve => setImmediate(resolve));
          continue;
        }
        const report = parseJson(row.report_json as string) as { kind: string; value?: import("./contract/index.js").Json };
        if (report.kind === "report" && report.value !== undefined) {
          // This pre-activation scan only reaches inline snapshots. Inspect
          // their persisted evidence without requiring an active coordinator;
          // responseFloor runs again after activation to include late writes.
          const action = readInlineActionSnapshot(store, actionRow);
          const input = { attemptID: row.attempt_id as string, phase: row.phase as "execute" | "reconcile" };
          let projection = reportResponseBytes(store.domainID, row.principal_id as string, action, input, report.value, true);
          const cancellation = store.effectiveCancellation({ kind: "action", id: action.id });
          if (cancellation && !action.outcome) {
            const terminal: ActionView = { ...action, status: "settled", outcome: { kind: "cancelled", reason: cancellation.reason },
              cancelRequested: true, cancellation, settledAt: Number.MAX_SAFE_INTEGER,
              blocked: action.blocked ?? (action.executionCount > 0 ? { code: "RECONCILIATION_REQUIRED", message: "External effect remains unknown after cancellation",
                details: { actionID: action.id, attemptID: action.latestAttempt?.id ?? null, source: "cancellation" } } : null) };
            projection = Math.max(projection, reportResponseBytes(store.domainID, row.principal_id as string, terminal, input, report.value, true));
          }
          // The query may use an administrator's principal and a non-SDK
          // requestID. Their encoded strings together occur in the bounded
          // request body; reserve that complete request budget rather than
          // assuming the owner's identity and the SDK's UUID forever.
          const withQueryIdentity = projection + limits.maxPayloadBytes;
          if (!Number.isSafeInteger(withQueryIdentity)) throw harnessError("INVALID_ARGUMENT", "Stored report exceeds the supported response budget");
          largest = Math.max(largest ?? 0, withQueryIdentity);
        }
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      // Reports not yet received are bounded by their original Attempt limit,
      // not by the size of one public response. V2 transfers their eventual
      // complete observations in chunks; predicting copies of an unknown
      // future value here would make small existing records unstartable.
      // Pending input/output validations must remain deliverable to a node
      // after lowering deployment limits. Measure the entire assignment, not
      // just the stored payload. Claim requestID, binding, nodeID and
      // incarnation all occur in the same bounded claim request; reserve P
      // for these variable strings in the response.
      let validationID = "";
      for (;;) {
        const row = store.query(`SELECT j.id,j.round_id,j.payload_json FROM validation_jobs j JOIN prepare_rounds r ON r.id=j.round_id
          WHERE j.id>? AND j.state IN ('pending','assigned') AND r.state='pending' ORDER BY j.id LIMIT 1`, [validationID])[0];
        if (!row) break;
        validationID = row.id as string;
        const max = Number.MAX_SAFE_INTEGER, uuid = "00000000-0000-0000-0000-000000000000";
        const envelope = { protocolVersion: "harness/1", requestID: "", binding: { domainID: store.domainID, principalID: "" }, ok: true,
          result: { assignment: { jobID: row.id, roundID: row.round_id, payload: parseJson(row.payload_json as string), assignmentID: uuid,
            assignmentEpoch: max, nodeID: "", incarnation: "", deadlineAt: max, roundDeadlineAt: max } } };
        const projection = Buffer.byteLength(canonicalJson(envelope), "utf8") + limits.maxPayloadBytes;
        if (!Number.isSafeInteger(projection)) throw harnessError("INVALID_ARGUMENT", "Stored validation exceeds the supported response budget");
        largest = Math.max(largest ?? 0, projection);
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      // Business assignments duplicate the claiming node and worker identity
      // in the lease. Both copies plus the response binding/request ID are
      // bounded by twice the complete incoming claim request.
      for (const scan of [
        { table: "driver_jobs", tag: "driver", sql: "SELECT j.id,j.run_id,j.principal_id,j.input_json,j.input_encoding,f.input_json frame_input_json,f.input_encoding frame_input_encoding,f.input_operation_id frame_input_operation_id FROM driver_jobs j JOIN frames f ON f.id=j.id AND f.run_id=j.run_id AND f.state='open' JOIN work_items w ON w.kind='run' AND w.subject_id=f.run_id AND w.stage='driver' WHERE j.id>? AND j.state IN ('pending','assigned','accepted') ORDER BY j.id LIMIT 1" },
        { table: "finalize_jobs", tag: "finalize", sql: "SELECT j.id,j.run_id,j.principal_id,j.input_json,j.input_encoding,f.input_json frame_input_json,f.input_encoding frame_input_encoding,f.input_operation_id frame_input_operation_id FROM finalize_jobs j JOIN frames f ON f.id=j.id AND f.run_id=j.run_id AND f.state='open' JOIN work_items w ON w.kind='run' AND w.subject_id=f.run_id AND w.stage IN ('driver','finalize') WHERE j.id>? AND j.state IN ('pending','assigned','accepted') ORDER BY j.id LIMIT 1" },
        { table: "start_jobs", tag: "start", sql: "SELECT j.id,j.id AS run_id,j.principal_id,j.agent_id,j.agent_version FROM start_jobs j WHERE j.id>? AND j.state IN ('pending','assigned','accepted') AND EXISTS (SELECT 1 FROM runs r JOIN work_items w ON w.kind='run' AND w.subject_id=r.id AND w.stage='start' WHERE r.id=j.id AND r.core_json IS NULL) ORDER BY j.id LIMIT 1" },
        { table: "wake_jobs", tag: "wake", sql: "SELECT j.id,j.run_id,j.principal_id,j.agent_id,j.agent_version FROM wake_jobs j WHERE j.id>? AND j.state IN ('pending','assigned','accepted') AND EXISTS (SELECT 1 FROM waits t JOIN work_items w ON w.kind='run' AND w.subject_id=t.run_id AND w.stage='wake' WHERE t.id=j.id AND t.run_id=j.run_id AND t.winner IS NOT NULL AND t.resumed_frame_id IS NULL) ORDER BY j.id LIMIT 1" },
      ]) {
        let jobID = "";
        for (;;) {
          const row = store.query(scan.sql, [jobID])[0];
          if (!row) break;
          jobID = row.id as string;
          const max = Number.MAX_SAFE_INTEGER, uuid = "00000000-0000-0000-0000-000000000000";
          const lease = { kind: "run", subjectID: row.run_id, nodeID: "", workerPrincipalID: "", incarnation: "", epoch: max, expiresAt: max, attemptID: null, attemptNumber: null };
          const identity = { jobID: row.id, runID: row.run_id, principalID: row.principal_id, assignmentID: uuid, assignmentEpoch: max,
            nodeID: "", incarnation: "", deadlineAt: max, lease, leaseTTLms: max };
          const frame = { id: row.id, run_id: row.run_id, input_json: row.frame_input_json, input_encoding: row.frame_input_encoding, input_operation_id: row.frame_input_operation_id };
          const frameInput = scan.tag === "driver" || scan.tag === "finalize" ? frameJobInput(row, frame) : null;
          if ((scan.tag === "driver" || scan.tag === "finalize") && frameInput === null) throw harnessError("INVALID_ARGUMENT", `Stored ${scan.table} frame input is unavailable`);
          let input: unknown = null, referencedLength: number | undefined;
          if (frameInput !== null) {
            if (frame.input_encoding === "inline") {
              if (frame.input_operation_id !== null) throw harnessError("INVALID_ARGUMENT", "Stored inline frame discriminator is invalid");
              input = readRunFrame(store, parseJson(frameInput));
            } else if (frame.input_encoding === "wait-ref-v1" && store.coordinatorEpoch === 0) {
              // Before authority, use only the declared length to reserve the
              // response floor. The fenced pass reconstructs and verifies it.
              const descriptor = parseJson(frameInput);
              if (!descriptor || typeof descriptor !== "object" || Array.isArray(descriptor) || descriptor.encoding !== "frame-wait-ref-v1"
                  || typeof descriptor.length !== "number" || !Number.isSafeInteger(descriptor.length) || descriptor.length <= 0)
                throw harnessError("INVALID_ARGUMENT", "Stored frame reference length is invalid");
              referencedLength = descriptor.length;
            } else input = await readFrameInput(store, frame);
          }
          const assignment = scan.tag === "driver" || scan.tag === "finalize"
            ? { [scan.tag]: true, ...identity, input }
            : { [scan.tag]: true, ...identity, ...(scan.tag === "wake" ? { waitID: row.id } : {}), agent: { id: row.agent_id, version: row.agent_version } };
          const envelope = { protocolVersion: "harness/1", requestID: "", binding: { domainID: store.domainID, principalID: "" }, ok: true, result: { assignment } };
          let envelopeBytes = 0;
          for (const chunk of canonicalJsonChunks(envelope)) { envelopeBytes += chunk.length; await new Promise<void>(resolve => setImmediate(resolve)); }
          const projection = envelopeBytes + (referencedLength === undefined ? 0 : referencedLength - 4) + 2 * limits.maxPayloadBytes;
          if (!Number.isSafeInteger(projection)) throw harnessError("INVALID_ARGUMENT", `Stored ${scan.table} assignment exceeds the supported response budget`);
          largest = Math.max(largest ?? 0, projection);
          await new Promise<void>(resolve => setImmediate(resolve));
        }
      }
      let actionID = "";
      for (;;) {
        const row = store.query(`SELECT a.id,a.id action_id,a.run_id,a.revision,a.snapshot_json,a.snapshot_projection_id,
            length(CAST(a.snapshot_json AS BLOB)) a,r.snapshot_json run_json,r.principal_id FROM actions a JOIN runs r ON r.id=a.run_id
          JOIN work_items w ON w.kind='action' AND w.subject_id=a.id
          WHERE a.id>? AND w.stage IN ('accepted','running','recheck') ORDER BY a.id LIMIT 1`, [actionID])[0];
        if (!row) break;
        actionID = row.id as string;
        let action: Pick<ActionView, "id" | "runID" | "request" | "checkpoint">;
        if (row.snapshot_projection_id !== null) {
          await projectedActionBytes(row);
          const projection = store.query("SELECT template_json FROM value_projections WHERE id=? AND action_id=?", [row.snapshot_projection_id as string, row.id as string])[0];
          const template = projection && typeof projection.template_json === "string" ? parseJson(projection.template_json) : null;
          if (!template || typeof template !== "object" || Array.isArray(template) || !template.request || typeof template.request !== "object"
              || Array.isArray(template.request) || !Object.hasOwn(template, "checkpoint")) throw harnessError("INVALID_ARGUMENT", "Stored Action assignment is unavailable");
          action = { id: row.id as string, runID: row.run_id as string, request: template.request as unknown as ActionView["request"],
            checkpoint: template.checkpoint as unknown as ActionView["checkpoint"] };
        } else action = expandActionRequests(store, parseJson(row.snapshot_json as string)) as unknown as ActionView;
        const run = parseJson(row.run_json as string) as { rootID: string };
        if (action.request.kind === "capacity") {
          const max = Number.MAX_SAFE_INTEGER, uuid = "00000000-0000-0000-0000-000000000000";
          const lease = { kind: "action", subjectID: action.id, nodeID: "", workerPrincipalID: "", incarnation: "", epoch: max, expiresAt: max, attemptID: uuid, attemptNumber: max };
          const assignment = { action: true, runID: action.runID, rootID: run.rootID, actionID: action.id, principalID: row.principal_id,
            request: action.request, checkpoint: action.checkpoint, mode: "automatic-recovery", recovering: false, stage: "reconcile",
            attemptID: uuid, attemptNumber: max, assignmentID: uuid, nodeID: "", incarnation: "", lease, leaseTTLms: max };
          const envelope = { protocolVersion: "harness/1", requestID: "", binding: { domainID: store.domainID, principalID: "" }, ok: true, result: { assignment } };
          let envelopeBytes = 0;
          for (const part of canonicalJsonChunks(envelope)) { envelopeBytes += part.length; await new Promise<void>(resolve => setImmediate(resolve)); }
          const projection = envelopeBytes + checkpointValueByteDelta(store, action.id, action.checkpoint as unknown as import("./contract/index.js").Json) + 2 * limits.maxPayloadBytes;
          if (!Number.isSafeInteger(projection)) throw harnessError("INVALID_ARGUMENT", "Stored Action assignment exceeds the supported response budget");
          largest = Math.max(largest ?? 0, projection);
        }
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      if (largest === null) return { bytes: watermark, retainedReportLimit };
      const estimate = Math.max(largest, largestSnapshot + largestCancellation + 128);
      if (!Number.isSafeInteger(estimate)) throw harnessError("INVALID_ARGUMENT", "Stored record exceeds the supported response budget");
      return { bytes: Math.max(watermark, watermark ? 0 : 4_194_304, estimate), retainedReportLimit };
    };
    const values = new ValueStore(store, valueQuota);
    // Verify existing reservations before taking authority; recheck after
    // fencing because an older coordinator could still have committed work.
    values.verifyReservation();
    const firstFloor = await responseFloor();
    if (valueQuota < firstFloor.retainedReportLimit)
      throw harnessError("INVALID_ARGUMENT", "Value store quota is below an advertised or retained Attempt value limit");
    limits.maxResponseBytes = Math.max(limits.maxResponseBytes, firstFloor.bytes);
    // Canonical responses are produced as JavaScript strings. In the ASCII
    // worst case the response byte count equals its UTF-16 code-unit count.
    // This runtime ceiling is necessary, but it is not a reservation of RAM.
    if (limits.maxResponseBytes > bufferConstants.MAX_STRING_LENGTH) {
      throw harnessError("INVALID_ARGUMENT", "Response limit exceeds the runtime JSON string ceiling");
    }
    const domainInfo: Omit<DomainInfo, "principalID"> = {
      ...limits, domainID: store.domainID, protocolVersion: "harness/1", idempotencyRetention: "until-domain-reset",
    };
    // The listening endpoint is gated until the final synchronous, one-way activation.
    const submissions = createSubmitService(store, { valueQuota, clock: nodeOptions.clock, prepareTimeoutMs: preparation.prepareTimeoutMs ?? 30_000,
      retryAfterMs: preparation.retryAfterMs ?? 1_000, pollMs: preparation.pollMs ?? nodeOptions.pollMs,
      defaultLimits: limits.defaultLimits, maxRunLimits, maxValueBytes: limits.maxValueBytes, maxFrameBytes: limits.maxFrameBytes, maxResponseBytes: limits.maxResponseBytes,
      authorizePrincipal: (principalID, method, kind, ref) => authentication.authorizePrincipal(principalID, method, kind, ref) });
    const authorizeWorker = (grant: BusinessLeaseGrant) => {
        const definitions = store.query("SELECT d.kind,d.id,d.version FROM node_definitions d JOIN node_sessions s ON s.incarnation=d.incarnation WHERE s.incarnation=? AND s.node_id=? AND s.principal_id=?", [grant.incarnation, grant.nodeID, grant.workerPrincipalID]);
        if (!definitions.length) throw harnessError("FORBIDDEN", "Worker has no assembled definitions");
        authentication.authorizeWorkerPrincipal(grant.workerPrincipalID, grant.nodeID, definitions.map(definition => ({ kind: definition.kind as "agent" | "capacity" | "policy", ref: { id: definition.id as string, version: definition.version as string } })));
      };
    const authorizeRecheck = (actionID: string) => {
      const row = store.query("SELECT q.principal_id,q.request_key,c.operation,c.status,c.request_json,r.principal_id owner_id FROM action_recheck_requests q JOIN commands c ON c.principal_id=q.principal_id AND c.request_key=q.request_key JOIN actions a ON a.id=q.action_id JOIN runs r ON r.id=a.run_id WHERE q.action_id=?", [actionID])[0];
      if (!row || row.operation !== "recheckAction" || row.status !== "applied") throw harnessError("FORBIDDEN", "Recheck management identity is unavailable");
      const request = parseJson(row.request_json as string) as { actionID?: string; requestKey?: string };
      const action = readActionControl(store, actionID);
      if (request.actionID !== actionID || request.requestKey !== row.request_key || action.id !== actionID || action.kind !== "capacity") throw harnessError("FORBIDDEN", "Recheck management identity is inconsistent");
      authentication.authorizeManagementPrincipal(row.principal_id as string, row.owner_id as string, "capacity", action.target, "admin.recheckAction");
      return { sql: "UPDATE action_recheck_requests SET request_key=? WHERE action_id=? AND principal_id=? AND request_key=?", params: [row.request_key as string, actionID, row.principal_id as string, row.request_key as string], expectChanges: 1 };
    };
    const authorizeModelRetry = (runID: string, target: import("./contract/index.js").Ref) => {
      authorizeModelRef(target);
      const owner = store.query("SELECT principal_id,snapshot_json FROM runs WHERE id=?", [runID])[0];
      if (!owner) throw harnessError("FORBIDDEN", "Original model caller is unavailable");
      authentication.authorizePrincipal(owner.principal_id as string, "client.submit", "capacity", target);
      authentication.authorizePrincipal(owner.principal_id as string, "client.submit", "agent", readRunSnapshot(store, owner.snapshot_json).agent);
    };
    const authorizeRetry = (actionID: string) => {
      const row = store.query("SELECT w.stage,w.payload_json,r.principal_id owner_id FROM work_items w JOIN actions a ON a.id=w.subject_id JOIN runs r ON r.id=a.run_id WHERE w.id=? AND w.kind='operator_retry' AND w.subject_id=?", [`operator.retry:${actionID}`, actionID])[0];
      if (!row || !["pending", "granted"].includes(row.stage as string)) throw harnessError("FORBIDDEN", "Operator retry authority is unavailable");
      const payload = parseJson(row.payload_json as string) as { principalID?: string; requestKey?: string; reportID?: string };
      const action = readActionControl(store, actionID);
      if (!payload.principalID || !payload.requestKey || !payload.reportID || action.id !== actionID || action.kind !== "capacity") throw harnessError("FORBIDDEN", "Operator retry identity is inconsistent");
      const command = store.query("SELECT request_json,status FROM commands WHERE principal_id=? AND request_key=? AND operation='resolveAction'", [payload.principalID, payload.requestKey])[0];
      const request = command && parseJson(command.request_json as string) as { actionID?: string; resolution?: { kind?: string; acceptDuplicateCharges?: boolean } };
      if (!command || command.status !== "applied" || !request || request.actionID !== actionID || !["allow_retry", "allow_model_retry"].includes(request.resolution?.kind ?? "")) throw harnessError("FORBIDDEN", "Operator retry command is unavailable");
      if (request.resolution?.kind === "allow_model_retry") {
        if (request.resolution.acceptDuplicateCharges !== true) throw harnessError("FORBIDDEN", "Model risk acceptance is absent");
        authorizeModelRetry(action.runID, action.target);
      }
      authentication.authorizeManagementPrincipal(payload.principalID, row.owner_id as string, "capacity", action.target);
      return { sql: "UPDATE work_items SET stage=? WHERE id=? AND kind='operator_retry' AND subject_id=? AND stage=? AND payload_json=?", params: [row.stage as string, `operator.retry:${actionID}`, actionID, row.stage as string, row.payload_json as string], expectChanges: 1 };
    };
    const runs = createRunService(store, { clock: nodeOptions.clock, pollMs: preparation.pollMs ?? nodeOptions.pollMs, prepareTimeoutMs: preparation.prepareTimeoutMs ?? 30_000, retryAfterMs: preparation.retryAfterMs ?? 1_000, maxFrameBytes: limits.maxFrameBytes, maxValueBytes: limits.maxValueBytes, maxResponseBytes: limits.maxResponseBytes, enableReferencedFrameInput: testOnly?.enableBoundReportTransfer === true, valueQuota, authorizePrincipal: (principalID, method, kind, ref) => authentication.authorizePrincipal(principalID, method, kind, ref), authorizeWorker });
    const leases = new BusinessLeaseService(store, { clock: nodeOptions.clock, ttlMs: nodeOptions.assignmentTTLms, authorizeRecheck });
    const actions = new ActionService(store, { clock: nodeOptions.clock, maxValueBytes: limits.maxValueBytes, maxPayloadBytes: limits.maxPayloadBytes, maxResponseBytes: limits.maxResponseBytes, valueQuota, canDispatchRun: runs.canExecuteAction, authorizeRecheck, authorizeRetry });
    const reports = createReportService(store, { clock: nodeOptions.clock, pollMs: preparation.pollMs ?? nodeOptions.pollMs, prepareTimeoutMs: preparation.prepareTimeoutMs ?? 30_000,
      retryAfterMs: preparation.retryAfterMs ?? 1_000, authorizePrincipal: (principalID, method, kind, ref) => authentication.authorizePrincipal(principalID, method, kind, ref), authorizeWorker,
      maxResponseBytes: limits.maxResponseBytes,
      authorizeOperator(principalID, _request, action) {
        const run = store.query("SELECT principal_id FROM runs WHERE id=?", [action.runID])[0];
        if (!run) throw harnessError("FORBIDDEN", "Action owner is unavailable");
        authentication.authorizeManagementPrincipal(principalID, run.principal_id as string, action.request.kind, action.request.target);
      }, canDispatchRun: runs.canExecuteAction, authorizeRecheck });
    const authorizeValidation = (payload: ValidationPayload) => {
      if (payload.operation !== "admin.resolveAction") return authentication.authorizePrincipal(payload.principalID, payload.operation, payload.kind, payload.ref);
      const context = (expandDispatchPayload(payload) as unknown as ValidationPayload).context;
      if (payload.phase !== "report-output" || payload.kind !== "capacity" || !context || typeof context !== "object" || Array.isArray(context)
          || context.reportPhase !== "operator" || typeof context.actionID !== "string" || typeof context.ownerPrincipalID !== "string"
          || (!context.operatorSource && (!context.command || typeof context.command !== "object" || Array.isArray(context.command) || typeof context.command.requestKey !== "string"))) throw harnessError("FORBIDDEN", "Operator validation binding is invalid");
      const source = context.operatorSource as unknown as import("./persistence/index.js").CommandValueSource | undefined;
      const key = source?.requestKey ?? (context.command as Record<string, Json>).requestKey as string;
      const row = store.query("SELECT c.request_json,c.status,a.request_json action_request_json,r.snapshot_json run_json,r.principal_id owner_id,r.id run_id FROM commands c JOIN actions a ON a.id=? JOIN runs r ON r.id=a.run_id WHERE c.principal_id=? AND c.request_key=? AND c.operation='resolveAction'", [context.actionID, payload.principalID, key])[0];
      if (!row || row.status !== "pending" || row.owner_id !== context.ownerPrincipalID || row.run_id !== context.runID) throw harnessError("FORBIDDEN", "Operator command or owner is unavailable");
      if (source) { if (source.principalID !== payload.principalID || source.actionID !== context.actionID || canonicalJson(source) !== canonicalJson(commandSource(row.request_json))) throw harnessError("FORBIDDEN", "Operator source differs from its command"); verifyCommandValue(store, source); }
      else if (row.request_json !== canonicalJson(context.command)) throw harnessError("FORBIDDEN", "Operator command differs from its source");
      const command = commandRequestMetadata(row.request_json);
      const action = readActionControl(store, context.actionID);
      const run = parseJson(row.run_json as string) as { id?: string; rootID?: string };
      const admission = store.query("SELECT data_json FROM history WHERE subject_id=? AND type='action.accepted' ORDER BY sequence LIMIT 1", [action.id])[0];
      const accepted = admission && parseJson(admission.data_json as string) as { frameID?: string };
      if (action.id !== context.actionID || action.revision !== command.expectedRevision || action.kind !== "capacity"
          || run.id !== action.runID || run.rootID !== context.rootID || accepted?.frameID !== context.frameID
          || context.reportID !== `resolve:${canonicalJson([payload.principalID, command.requestKey])}`
          || !fixedActionValidationMatches(store, context.actionID, { ...payload, context }, row.action_request_json)
          || (context.prepared as Record<string, Json>).kind !== action.kind
          || canonicalJson((context.prepared as Record<string, Json>).target) !== canonicalJson(action.target)
          || canonicalJson(action.target) !== canonicalJson(payload.ref)) throw harnessError("FORBIDDEN", "Operator Action has changed");
      authentication.authorizeManagementPrincipal(payload.principalID, row.owner_id as string, "capacity", action.target);
    };
    const nodes = new NodeService(store, nodeOptions, authorizeValidation, limits.maxValueBytes, runs.canDrive, leases, actions, runs.canWake, valueQuota);
    // Private integration seam: ordinary coordinator startup never advertises v4.
    const boundReportTransfer = testOnly?.enableBoundReportTransfer === true
      ? new BoundReportTransfer(values, new ReportTransferBindings(store, values), {
        prepare: (source, principal, assertWorker) => nodes.prepareReportTransferSource(source, principal, assertWorker),
        preparePublished: (source, principal, assertWorker) => nodes.preparePublishedReportTransferSource(source, principal, assertWorker),
        publish: (params, ref, proof, transfer, principal, assertWorker) => nodes.recordStagedReport(params, ref, proof, transfer, principal, assertWorker),
      }, transferGate) : undefined;
    const queries = createQueryAccess(store, limits);
    const cancellations = createCancelService(store, { clock: nodeOptions.clock, maxResponseBytes: limits.maxResponseBytes, authorizeRun: queries.authorizeRunControl });
    const versions = createVersionService(store, nodeOptions.clock, { ...limits, valueQuota });
    const management = createManagementService(store, { clock: nodeOptions.clock, valueQuota, prepareTimeoutMs: preparation.prepareTimeoutMs ?? 30_000, authorizeRun: queries.authorizeRun, authorizeRunControl: queries.authorizeRunControl });
    const rechecks = createRecheckService(store, { clock: nodeOptions.clock, authorizeAction: queries.authorizeAction, authorizeActionControl: queries.authorizeActionControl });
    const resolutions = createResolveService(store, { authorizeModelRetry: action => authorizeModelRetry(action.runID, action.request.target), valueQuota, clock: nodeOptions.clock, pollMs: preparation.pollMs ?? nodeOptions.pollMs, maxValueBytes: limits.maxValueBytes, maxResponseBytes: limits.maxResponseBytes,
      prepareTimeoutMs: preparation.prepareTimeoutMs ?? 30_000, retryAfterMs: preparation.retryAfterMs ?? 1_000, authorizeAction: queries.authorizeAction, authorizeActionControl: queries.authorizeActionControl,
      authorizeOperator(principalID, ownerID, action) { authentication.authorizeManagementPrincipal(principalID, ownerID, action.request.kind, action.request.target); } });
    server = await startRPCServer({ host, port, authentication, domainInfo, maxNodeValueBytes: firstFloor.retainedReportLimit,
      isReady: () => active,
      ...(boundReportTransfer ? {
        boundReportTransfer, reportStaging: new CanonicalReportStaging(values, Number.MAX_SAFE_INTEGER, transferGate), advertiseBoundReportTransfer: true,
        authorizeStagedWorker(nodeID: string | undefined, incarnation: string | undefined, principal: import("./message/index.js").PrincipalGrant) {
          if (nodeID !== undefined && incarnation !== undefined) for (const definition of nodes.getDefinitions(nodeID, incarnation))
            authorizeDefinition(principal, definition.kind, definition.ref.id, definition.ref.version);
        },
      } : {}),
      fragmentStagingParent: dirname(resolve(databasePath)),
      authorizeFragment: queries.authorizeFragment,
      prepareFragmentAuthorization: queries.prepareFragmentAuthorization,
      authorizeFragmentDescriptor: queries.authorizeFragmentDescriptor,
      authorizeErrorFragment: queries.authorizeErrorFragment,
      async authorizeNodeFragment(request, result, principal, revalidate) {
        const assertWorker = () => {
          const current = revalidate();
          for (const definition of nodes.getDefinitions(request.params.nodeID, request.params.incarnation))
            authorizeDefinition(current, definition.kind, definition.ref.id, definition.ref.version);
        };
        if (request.method === "claim") await nodes.authorizeClaimFragment(request.params, result, principal.principalID, assertWorker);
        else if (request.method === "checkpointAction" || request.method === "readCheckpoint") nodes.authorizeCheckpointFragment(request, result, principal.principalID, assertWorker);
        else if (request.method === "readValidationValue") nodes.authorizeValidationValueFragment(request.params, result as import("./contract/index.js").NodeRPCMap["readValidationValue"]["result"], principal.principalID, assertWorker);
        else throw harnessError("FORBIDDEN", "Unsupported node response fragment");
      },
      authorizeNodeFragmentFinal(request, result, principal) {
        const assertWorker = () => {
          for (const definition of nodes.getDefinitions(request.params.nodeID, request.params.incarnation))
            authorizeDefinition(principal, definition.kind, definition.ref.id, definition.ref.version);
        };
        if (request.method === "claim") nodes.authorizeClaimFragmentCurrent(request.params, result, principal.principalID, assertWorker);
        else if (request.method === "checkpointAction" || request.method === "readCheckpoint") nodes.authorizeCheckpointFragment(request, result, principal.principalID, assertWorker);
        else if (request.method === "readValidationValue") nodes.authorizeValidationValueFragment(request.params, result as import("./contract/index.js").NodeRPCMap["readValidationValue"]["result"], principal.principalID, assertWorker);
        else throw harnessError("FORBIDDEN", "Unsupported node response fragment");
      },
      handle: (request, principal, revalidate, context) => request.method === "client.submit" ? submissions.submit(request.params, principal, revalidate, request.requestID, context?.fragmentedResponses === true)
        : request.method === "client.requestCancel" ? cancellations.cancel(request.params, principal, revalidate, request.requestID, context?.fragmentedResponses === true)
        : request.method === "admin.adoptVersion" ? versions.adoptVersion(request.params, principal, revalidate, request.requestID, context?.fragmentedResponses === true)
        : request.method === "admin.resumeRun" ? management.resumeRun(request.params, principal, revalidate)
        : request.method === "admin.recheckAction" ? rechecks.recheckAction(request.params, principal, revalidate)
        : request.method === "admin.resolveAction" ? resolutions.resolveAction(request.params, principal, revalidate, context?.fragmentedResponses === true) : queries.handle(request, principal, revalidate, context),
      assertAuthority: () => {
        if (!active) throw harnessError("EXECUTION_FENCED", "Coordinator activation is incomplete");
        store.assertAuthority();
      },
      handleNode(request, principal, revalidate) {
        // Check the current worker's entire installed set on every operation, including renewal/replay.
        for (const definition of request.method === "register" ? request.params.definitions : nodes.getDefinitions(request.params.nodeID, request.params.incarnation)) {
          authorizeDefinition(principal, definition.kind, definition.ref.id, definition.ref.version);
        }
        return nodes.handle(request, principal.principalID, () => {
          const current = revalidate();
          for (const definition of request.method === "register" ? request.params.definitions : nodes.getDefinitions(request.params.nodeID, request.params.incarnation)) {
            authorizeDefinition(current, definition.kind, definition.ref.id, definition.ref.version);
          }
        });
      },
    });
    store.claimAuthority();
    values.verifyReservation();
    // Resume only cleanups which already have a durable authorized retirement
    // receipt. No TTL or stale epoch by itself authorizes deleting content.
    await new ReportTransferBindings(store, values).resumeRetirements();
    await new ReportTransferBindings(store, values).resumeDiagnosticRetirements();
    await resumeDirectDiagnosticRetirements(store, values);
    await values.resumeUnboundRetirements();
    // A competing coordinator could have raised the floor after our read. Do
    // not publish an endpoint whose transport was initialized below it.
    const finalFloor = await responseFloor();
    if (finalFloor.bytes > limits.maxResponseBytes || finalFloor.retainedReportLimit > firstFloor.retainedReportLimit)
      throw harnessError("STATE_CONFLICT", "Response or Attempt compatibility floor changed during startup; retry");
    store.assertAuthority();
    const previousResponseLimit = store.query("SELECT bytes FROM response_limit_watermark WHERE singleton=1")[0]!.bytes as number;
    if (previousResponseLimit !== limits.maxResponseBytes) store.commit({ operationID: `response-limit:${randomUUID()}`, kind: "domain.response_limit", subjectID: store.domainID,
      request: { previous: previousResponseLimit, bytes: limits.maxResponseBytes }, evidence: null, result: null,
      writes: [{ sql: "UPDATE response_limit_watermark SET bytes=? WHERE singleton=1 AND bytes=?", params: [limits.maxResponseBytes, previousResponseLimit], expectChanges: 1 }] });
    submissions.start();
    runs.start();
    reports.start();
    resolutions.start();
    active = true;
    const activeServer = server;
    let closing: Promise<void> | undefined;
    return { endpoint: activeServer.endpoint,
      inspectRecovery() {
        const storage = store.inspectRecovery(), execution = runs.inspectRecovery();
        const scope = storage.authority !== "current" || storage.frozen ? "coordinator" : execution.quarantinedRunIDs.length ? "runs" : "none";
        const nextStep = storage.authority === "superseded" ? "use-current-coordinator" : scope !== "none" && storage.authority !== "closed" ? "inspect-evidence" : "none";
        return { scope, nextStep, storage, execution };
      },
      close() { submissions.close(); runs.close(); reports.close(); resolutions.close(); return closing ??= activeServer.close().finally(() => store.close()); } };
  } catch (error) {
    try { await server?.close(); } catch { /* Preserve the startup/activation failure. */ }
    try { store.close(); } catch { /* No service is published after failure. */ }
    throw error;
  }
}

function configuredMaxRunLimits(supplied: Partial<RunLimits> | undefined, defaults: RunLimits): Partial<RunLimits> {
  const value = copyJson(supplied ?? {});
  if (!value || typeof value !== "object" || Array.isArray(value)
      || Object.keys(value).some(key => !Object.hasOwn(defaults, key))) throw harnessError("INVALID_ARGUMENT", "Invalid maximum Run limits");
  for (const [key, limit] of Object.entries(value)) {
    if (key === "defaultWaitTimeoutMs" && limit === null) continue;
    if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < (key === "defaultWaitTimeoutMs" ? 0 : 1)) {
      throw harnessError("INVALID_ARGUMENT", "Invalid maximum Run limits");
    }
    const current = defaults[key as keyof RunLimits];
    if (current === null || current > limit) throw harnessError("INVALID_ARGUMENT", "Default Run limit exceeds deployment maximum", { field: key });
  }
  return value as Partial<RunLimits>;
}

function configuredLimits(limits: CoordinatorOptions["limits"]): PublishedLimits {
  const supplied = copyJson(limits ?? {});
  const defaults: PublishedLimits = {
    defaultLimits: { maxDepth: 16, maxActions: 1_000, maxFrames: 1_000, maxExecutionsPerAction: 3, defaultWaitTimeoutMs: null },
    maxPayloadBytes: 1_048_576, maxValueBytes: 262_144, maxFrameBytes: 1_048_576,
    maxResponseBytes: 4_194_304, maxPageSize: 100,
  };
  if (!supplied || typeof supplied !== "object" || Array.isArray(supplied) || Object.keys(supplied).some(key => !Object.hasOwn(defaults, key))) {
    throw harnessError("INVALID_ARGUMENT", "Invalid coordinator limits");
  }
  const checked = validateRPCResponse({ protocolVersion: "harness/1", requestID: "configuration",
    binding: { domainID: "configuration", principalID: "configuration" }, ok: true,
    result: { ...defaults, ...supplied, domainID: "configuration", principalID: "configuration", protocolVersion: "harness/1", idempotencyRetention: "until-domain-reset" },
  }, "session.getInfo");
  if (!checked.ok || checked.result.maxResponseBytes < 1_024) throw harnessError("INVALID_ARGUMENT", "maxResponseBytes must accommodate a protocol error envelope");
  const { defaultLimits, maxPayloadBytes, maxValueBytes, maxFrameBytes, maxResponseBytes, maxPageSize } = checked.result;
  return { defaultLimits, maxPayloadBytes, maxValueBytes, maxFrameBytes, maxResponseBytes, maxPageSize };
}
