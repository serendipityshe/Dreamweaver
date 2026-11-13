import { encodeRunInput, sameChildValue } from "../persistence/index.js";
import { readValidationConclusionAsync } from "../persistence/index.js";
import { stageSubmitInput, abandonSubmitInput, submitPublicationWrites, encodeSubmitRequest, submitRequestMetadata, readSubmitRequest, submitValidationPayload, verifySubmitInput, type SubmitInputSource } from "../persistence/index.js";
import { randomUUID } from "node:crypto";
import { assertJsonValueSize, canonicalJson, canonicalJsonDigest, canonicalJsonChunks, copyJson, harnessError, HarnessError, jsonEqual, parseJson,
  type AdvanceInput, type ErrorInfo, type Json, type Ref, type RunLimits, type RunView, type SubmitReceipt, type SubmitRequest, type ValidationPayload } from "../contract/index.js";
import { authorizeDefinition, authorizeOperation, type PrincipalGrant } from "../message/index.js";
import { type SQLWrite, type SQLiteStore, type CommitBatch } from "../persistence/index.js";

type Row = ReturnType<SQLiteStore["query"]>[number];
type CommandState = { status: "missing" | "pending" } | { status: "applied"; receipt: SubmitReceipt } | { status: "rejected"; error: ErrorInfo };
export interface SubmitServiceOptions {
  clock: { now(): number };
  prepareTimeoutMs: number;
  retryAfterMs: number;
  pollMs: number;
  defaultLimits: RunLimits;
  maxValueBytes: number;
  maxFrameBytes: number;
  maxResponseBytes: number;
  valueQuota?: number;
  maxRunLimits?: Partial<RunLimits>;
  authorizePrincipal(principalID: string, method: "client.submit", kind: "agent", ref: Ref): void;
}
const write = (sql: string, params: SQLWrite["params"], expectChanges = 1): SQLWrite => ({ sql, params, expectChanges });
const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const jsonBytes = (value: unknown) => { let bytes = 0; for (const part of canonicalJsonChunks(value)) bytes += part.length; return bytes; };
const identity = (kind: string, principal: string, key: string): string => `${kind}:${canonicalJson([principal, key])}`;

/** Root admission and durable bounded preparation; never executes plugin code. */
export function createSubmitService(store: SQLiteStore, options: SubmitServiceOptions) {
  for (const value of [options.prepareTimeoutMs, options.retryAfterMs, options.pollMs, options.maxValueBytes, options.maxFrameBytes, options.maxResponseBytes]) {
    if (!Number.isSafeInteger(value) || value <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid submit preparation configuration");
  }
  let timer: ReturnType<typeof setInterval> | undefined;
  let scheduled: ReturnType<typeof setImmediate> | undefined;
  let scanning = false;
  let cursor: [number, string, string] | undefined;
  const scanBatchSize = 16;
  let closed = false;
  const waiters = new Map<string, Set<() => void>>();
  const wake = (principal: string, key: string) => {
    const id = identity("wait", principal, key), waiting = waiters.get(id);
    waiters.delete(id);
    if (waiting) for (const resolve of waiting) resolve();
  };
  const wakeAll = () => { for (const waiting of waiters.values()) for (const resolve of waiting) resolve(); waiters.clear(); };
  const now = () => {
    const n = options.clock.now();
    if (!Number.isSafeInteger(n) || n < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock");
    return n;
  };
  function commit(kind: string, principal: string, key: string, semantic: unknown, writes: SQLWrite[], result: unknown, validBefore?: number, absent?: CommitBatch["absent"], events?: CommitBatch["events"], valuePublications?: CommitBatch["valuePublications"]) {
    return store.commit({ operationID: identity(kind, principal, key), kind, subjectID: identity("command", principal, key),
      request: json(semantic), writes, result: json(result), evidence: null, ...(validBefore === undefined ? {} : { validBefore }), ...(absent ? { absent } : {}), ...(events ? { events } : {}), ...(valuePublications ? { valuePublications } : {}) });
  }
  function command(principal: string, key: string): Row | undefined {
    return store.query("SELECT * FROM commands WHERE principal_id=? AND request_key=?", [principal, key])[0];
  }
  function resolved(request: SubmitRequest, authorize: (candidate: { ref: Ref; revision: number | null }) => void = () => {}): { ref: Ref; revision: number | null } {
    let version = request.agent.version;
    let revision: number | null = null;
    if (version === undefined) {
      const pointer = store.query("SELECT version,revision FROM version_pointers WHERE kind=? AND id=?", ["agent", request.agent.id])[0];
      if (!pointer) throw harnessError("VERSION_UNAVAILABLE", "No default Agent version is set", { missing: "pointer", id: request.agent.id });
      version = pointer.version as string;
      revision = pointer.revision as number;
    }
    const ref = { id: request.agent.id, version };
    authorize({ ref, revision });
    if (!store.query("SELECT id FROM definition_catalog WHERE kind=? AND id=? AND version=?", ["agent", ref.id, ref.version]).length) {
      throw harnessError("VERSION_UNAVAILABLE", "Agent version is not registered", { missing: "definition", id: ref.id, version: ref.version, pointerRevision: revision });
    }
    return { ref, revision };
  }
  function absence(error: HarnessError): CommitBatch["absent"] {
    const details = error.info.details as { missing?: string; id?: string; version?: string } | null;
    if (details?.missing === "pointer" && details.id) return [{ table: "version_pointers", kind: "agent", id: details.id }];
    if (details?.missing === "definition" && details.id && details.version) return [{ table: "definition_catalog", kind: "agent", id: details.id, version: details.version }];
    return undefined;
  }
  function payload(request: SubmitRequest, principalID: string, candidate: { ref: Ref; revision: number | null }, encoded?: unknown): ValidationPayload {
    return submitValidationPayload(request, principalID, candidate.ref, candidate.revision, encoded) as unknown as ValidationPayload;
  }
  function current(principalID: string, request: SubmitRequest, grant: PrincipalGrant): CommandState {
    store.assertAuthority();
    authorizeOperation(grant, "client.submit");
    authorizeDefinition(grant, "agent", request.agent.id, request.agent.version);
    const row = command(principalID, request.requestKey);
    if (!row) return { status: "missing" };
    if (row.operation === "submit") {
      const original = submitRequestMetadata(row.request_json);
      authorizeDefinition(grant, "agent", original.agent.id, original.agent.version);
      if (row.status === "applied") {
        const originalReceipt = decode<SubmitReceipt>(row.result_json);
        authorizeDefinition(grant, "agent", originalReceipt.agent.id, originalReceipt.agent.version);
      } else {
        const preparation = store.query("SELECT candidate_json FROM submit_preparations WHERE principal_id=? AND request_key=?", [principalID, request.requestKey])[0];
        if (preparation?.candidate_json) {
          const candidate = decode<Ref>(preparation.candidate_json);
          authorizeDefinition(grant, "agent", candidate.id, candidate.version);
        }
      }
    }
    if (row.operation !== "submit" || !sameChildValue(readSubmitRequest(store, principalID, request.requestKey, row.request_json), request)) throw harnessError("REQUEST_CONFLICT", "Request key is bound to a different command");
    if (row.status === "rejected") return { status: "rejected", error: decode<ErrorInfo>(row.error_json) };
    if (row.status === "applied") {
      const receipt = decode<SubmitReceipt>(row.result_json);
      authorizeDefinition(grant, "agent", receipt.agent.id, receipt.agent.version);
      return { status: "applied", receipt };
    }
    return { status: "pending" };
  }
  function checkLimits(limits: Partial<RunLimits>) {
    for (const [key, value] of Object.entries(limits)) {
      const maximum = options.maxRunLimits?.[key as keyof RunLimits];
      if (typeof maximum === "number" && (value === null || typeof value === "number" && value > maximum)) {
        throw harnessError("LIMIT_EXCEEDED", "Run limit exceeds the deployment range", { field: key, minimum: key === "defaultWaitTimeoutMs" ? 0 : 1, maximum });
      }
    }
  }
  function registerPrepared(request: SubmitRequest, principalID: string, grant: PrincipalGrant, responseBudget: number | null, fragmentedResponses: boolean, source: SubmitInputSource | null, stageEpoch: number) {
    if (store.coordinatorEpoch !== stageEpoch) throw harnessError("EXECUTION_FENCED", "Submit staging owner changed");
    const encoded = encodeSubmitRequest(request, source);
    const at = now();
    let failure: HarnessError | undefined;
    let candidate: { ref: Ref; revision: number | null } | undefined;
    try {
      candidate = resolved(request, selected => {
        authorizeDefinition(grant, "agent", selected.ref.id, selected.ref.version);
        options.authorizePrincipal(principalID, "client.submit", "agent", selected.ref);
      });
      assertJsonValueSize(request.input, options.maxValueBytes);
      assertJsonValueSize(request.config, options.maxValueBytes);
      checkLimits(request.limits ?? {});
    } catch (error) {
      if (!(error instanceof HarnessError) || !["LIMIT_EXCEEDED", "VERSION_UNAVAILABLE"].includes(error.info.code) || error.info.disposition !== "rejected") throw error;
      failure = error;
    }
    const fingerprint = canonicalJsonDigest(request);
    const commandWrite = write("INSERT INTO commands (principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
      [principalID, request.requestKey, "submit", { json: encoded }, fingerprint, failure ? "rejected" : "pending", null, failure ? { json: json(failure.info) } : null, at, at]);
    const writes = [...submitPublicationWrites(source, stageEpoch), commandWrite];
    if (!failure && candidate) {
      const roundID = randomUUID();
      const deadline = at + options.prepareTimeoutMs;
      if (!Number.isSafeInteger(deadline)) throw harnessError("LIMIT_EXCEEDED", "Preparation deadline exceeds supported range");
      const ownerID = identity("submit", principalID, request.requestKey);
      writes.push(write("INSERT INTO prepare_rounds (id,owner_kind,owner_id,principal_id,phase,number,prepare_timeout_ms,created_at,deadline_at,state) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [roundID, "command", ownerID, principalID, "input", 1, options.prepareTimeoutMs, at, deadline, "pending"]));
      writes.push(write("INSERT INTO submit_preparations (principal_id,request_key,round_id,candidate_json,pointer_revision,limits_json,response_budget,fragmented_response) VALUES (?,?,?,?,?,?,?,?)",
        [principalID, request.requestKey, roundID, { json: json(candidate.ref) }, candidate.revision, { json: json(request.limits ?? {}) }, responseBudget, fragmentedResponses ? 1 : 0]));
      writes.push(write("INSERT INTO validation_jobs (id,round_id,payload_json,state) VALUES (?,?,?,?)",
        [randomUUID(), roundID, { json: json(payload(request, principalID, candidate, encoded)) }, "pending"]));
      writes.push(write("INSERT INTO work_items (id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES (?,?,?,?,?,?,?,?)",
        [identity("submit.work", principalID, request.requestKey), "submit", ownerID, "pending", 0, at, { json: json({ principalID, requestKey: request.requestKey }) }, null]));
    }
    if (candidate?.revision !== null && candidate?.revision !== undefined) {
      writes.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [candidate.revision, "agent", candidate.ref.id, candidate.ref.version, candidate.revision]));
    }
    if (failure?.info.code === "VERSION_UNAVAILABLE" && request.agent.version === undefined) {
      const details = failure.info.details as { missing?: string; id?: string; version?: string; pointerRevision?: number } | null;
      if (details?.missing === "definition" && details.id && details.version && details.pointerRevision) writes.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [details.pointerRevision, "agent", details.id, details.version, details.pointerRevision]));
    }
    commit("submit.registered", principalID, request.requestKey, { request: encoded, candidate: candidate ?? null, failure: failure?.info ?? null, prepareTimeoutMs: options.prepareTimeoutMs }, writes, null, undefined, failure && absence(failure), undefined, source ? [source.value] : undefined);
    requestScan();
  }
  async function register(request: SubmitRequest, principalID: string, revalidate: () => PrincipalGrant, responseBudget: number | null, fragmentedResponses: boolean) {
    const stageEpoch = store.coordinatorEpoch;
    const assertCaller = () => {
      if (closed) throw harnessError("TRANSPORT_UNCERTAIN", "Coordinator stopped during submit staging");
      const grant = revalidate();
      if (grant.principalID !== principalID) throw harnessError("SESSION_MISMATCH", "Submit principal changed");
      authorizeOperation(grant, "client.submit"); authorizeDefinition(grant, "agent", request.agent.id, request.agent.version);
    };
    assertCaller();
    const source = await stageSubmitInput(store, principalID, request, options.valueQuota ?? Number.MAX_SAFE_INTEGER, assertCaller);
    try {
      assertCaller(); store.assertAuthority();
      if (store.coordinatorEpoch !== stageEpoch) throw harnessError("EXECUTION_FENCED", "Submit staging owner changed");
      const grant = revalidate();
      // Concurrent staging does not reserve the key. Only the admission transaction does.
      if (current(principalID, request, grant).status === "missing") registerPrepared(request, principalID, grant, responseBudget, fragmentedResponses, source, stageEpoch);
    } finally { abandonSubmitInput(store, source); }
  }
  function reject(row: Row, prep: Row, error: HarnessError, at: number, guard?: { candidate?: { ref: Ref; revision: number | null }; job?: Row; absent?: CommitBatch["absent"] }) {
    const principal = row.principal_id as string, key = row.request_key as string, round = prep.round_id as string;
    const jobs = store.query("SELECT id,state FROM validation_jobs WHERE round_id=? AND state<>?", [round, "superseded"]);
    const writes: SQLWrite[] = [write("UPDATE commands SET status=?,error_json=?,updated_at=? WHERE principal_id=? AND request_key=? AND status=? AND fingerprint=?",
      ["rejected", { json: json(error.info) }, at, principal, key, "pending", row.fingerprint as string]),
      write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at=?", ["expired", round, "pending", prep.deadline_at as number]),
      write("UPDATE work_items SET stage=? WHERE id=? AND stage=?", ["done", identity("submit.work", principal, key), "pending"])];
    if (guard?.candidate?.revision !== null && guard?.candidate?.revision !== undefined) {
      const candidate = guard.candidate!;
      writes.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [candidate.revision, "agent", candidate.ref.id, candidate.ref.version, candidate.revision]));
    }
    if (guard?.job) writes.push(write("UPDATE submit_preparations SET candidate_json=? WHERE principal_id=? AND request_key=? AND round_id=? AND candidate_json=? AND pointer_revision IS ?", [{ json: decode<Json>(prep.candidate_json) }, principal, key, round, prep.candidate_json as string, prep.pointer_revision as number | null]));
    if (guard?.job) writes.push(write("UPDATE validation_jobs SET state=? WHERE id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND payload_json=? AND report_json=?", ["accepted", guard.job.id as string, "accepted", guard.job.assignment_id as string, guard.job.assignment_epoch as number, guard.job.payload_json as string, guard.job.report_json as string]));
    for (const job of jobs) writes.push(write("UPDATE validation_jobs SET state=? WHERE id=? AND state=?", ["superseded", job.id as string, job.state as string]));
    commit("submit.rejected", principal, key, { request: decode(row.request_json), deadlineAt: prep.deadline_at, code: error.info.code, details: error.info.code === "VERSION_UNAVAILABLE" && at >= (prep.deadline_at as number) ? null : error.info.details, retryAfterMs: error.info.retryAfterMs, candidate: guard?.candidate ?? null }, writes, null, at < (prep.deadline_at as number) ? prep.deadline_at as number : undefined, guard?.absent);
    wake(principal, key);
  }
  async function process(row: Row) {
    store.assertAuthority();
    const principal = row.principal_id as string, key = row.request_key as string;
    let request = submitRequestMetadata(row.request_json);
    const prep = store.query("SELECT p.*,r.deadline_at,r.state AS round_state FROM submit_preparations p JOIN prepare_rounds r ON r.id=p.round_id WHERE p.principal_id=? AND p.request_key=?", [principal, key])[0];
    if (!prep || prep.round_state !== "pending") throw harnessError("INTERNAL_ERROR", "Pending submit has no active preparation");
    const at = now(), deadline = prep.deadline_at as number, round = prep.round_id as string;
    if (at >= deadline) {
      reject(row, prep, harnessError("VERSION_UNAVAILABLE", "Input preparation expired", { phase: "submit", roundID: round, deadlineAt: deadline }, "rejected", options.retryAfterMs), at);
      return;
    }
    let candidate: { ref: Ref; revision: number | null } | undefined;
    try { candidate = resolved(request, selected => {
      candidate = selected;
      options.authorizePrincipal(principal, "client.submit", "agent", selected.ref);
    }); }
    catch (error) {
      if (error instanceof HarnessError && error.info.code === "FORBIDDEN") {
        if (!candidate) throw error;
        reject(row, prep, error, at, { candidate }); return;
      }
      if (error instanceof HarnessError && error.info.code === "VERSION_UNAVAILABLE") {
        const details = error.info.details as { missing?: string; id?: string; version?: string; pointerRevision?: number } | null;
        const missingCandidate = details?.missing === "definition" && details.id && details.version && details.pointerRevision
          ? { ref: { id: details.id, version: details.version }, revision: details.pointerRevision } : undefined;
        reject(row, prep, error, at, { candidate: missingCandidate, absent: absence(error) }); return;
      }
      throw error;
    }
    const oldRef = decode<Ref>(prep.candidate_json);
    if (!jsonEqual(oldRef, candidate.ref) || prep.pointer_revision !== candidate.revision) {
      const jobs = store.query("SELECT id,state FROM validation_jobs WHERE round_id=? AND state<>?", [round, "superseded"]);
      const writes: SQLWrite[] = [write("UPDATE commands SET updated_at=? WHERE principal_id=? AND request_key=? AND status=? AND fingerprint=?", [at, principal, key, "pending", row.fingerprint as string]),
        write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["pending", round, "pending", at]),
        write("UPDATE submit_preparations SET candidate_json=?,pointer_revision=? WHERE principal_id=? AND request_key=? AND round_id=? AND candidate_json=? AND pointer_revision IS ?",
          [{ json: json(candidate.ref) }, candidate.revision, principal, key, round, prep.candidate_json as string, prep.pointer_revision as number | null])];
      if (candidate.revision !== null) writes.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [candidate.revision, "agent", candidate.ref.id, candidate.ref.version, candidate.revision]));
      for (const job of jobs) writes.push(write("UPDATE validation_jobs SET state=? WHERE id=? AND state=?", ["superseded", job.id as string, job.state as string]));
      writes.push(write("INSERT INTO validation_jobs (id,round_id,payload_json,state) VALUES (?,?,?,?)", [randomUUID(), round, { json: json(payload(request, principal, candidate, row.request_json)) }, "pending"]));
      // Identity includes the pointer revision, allowing several changes in one round.
      commit(`submit.reprepare:${candidate.revision}`, principal, key, { request: decode(row.request_json), previous: oldRef, candidate, deadlineAt: deadline }, writes, null, deadline);
      return;
    }
    const job = store.query("SELECT * FROM validation_jobs WHERE round_id=? AND state=?", [round, "accepted"])[0];
    if (!job) return;
    if (!jsonEqual(decode(job.payload_json), payload(request, principal, candidate, row.request_json))) throw harnessError("INTERNAL_ERROR", "Validation payload does not match preparation");
    const conclusion = await readValidationConclusionAsync(store, job.report_json, () => {
      if (closed) throw harnessError("EXECUTION_FENCED", "Submit service stopped during validation read");
      options.authorizePrincipal(principal, "client.submit", "agent", candidate!.ref);
    });
    if (!conclusion.valid) { reject(row, prep, harnessError("INVALID_ARGUMENT", "Agent input validation failed", json(conclusion.issue)), at, { candidate, job }); return; }
    request = readSubmitRequest(store, principal, key, row.request_json);
    const inputSource = verifySubmitInput(store, principal, key, row.request_json);
    const limits = { ...options.defaultLimits, ...decode<Partial<RunLimits>>(prep.limits_json) };
    try { checkLimits(limits); } catch (error) {
      if (error instanceof HarnessError && error.info.code === "LIMIT_EXCEEDED") { reject(row, prep, error, at, { candidate, job }); return; }
      throw error;
    }
    const runID = randomUUID();
    const receipt: SubmitReceipt = { runID, acceptedAt: at, agent: candidate.ref };
    // The first caller's transport overhead is durable but is not part of the
    // business request fingerprint. Repreparation retains the same budget.
    if (typeof prep.response_budget === "number" && Buffer.byteLength(canonicalJson(receipt), "utf8") > prep.response_budget) {
      reject(row, prep, harnessError("LIMIT_EXCEEDED", "Submit receipt cannot fit in the original response budget"), at, { candidate, job });
      return;
    }
    const view: RunView = { id: runID, rootID: runID, parentActionID: null, revision: 0, agent: candidate.ref,
      input: request.input, config: request.config, lifecycle: null, continuation: { entry: "start", state: null }, wait: null,
      outcome: null, cancelRequested: false, cancellation: null, blocked: null, limits, rootActionCount: 0, frameCount: 0,
      createdAt: at, updatedAt: at, completedAt: null };
    // The first frame's inputs are fixed at admission. Its eventual UUID has
    // the same encoded size as this one; do not accept a Run that is already
    // known to be unable to start under the current frame limit.
    const startInput: AdvanceInput = { frameID: randomUUID(), run: { id: view.id, rootID: view.rootID,
      parentActionID: view.parentActionID, agent: view.agent, input: view.input, config: view.config, limits: view.limits },
      continuation: view.continuation, wake: { kind: "start" } };
    if (jsonBytes(startInput) > options.maxFrameBytes) {
      reject(row, prep, harnessError("LIMIT_EXCEEDED", "Initial frame exceeds size limit", { maxFrameBytes: options.maxFrameBytes }), at, { candidate, job });
      return;
    }
    // Reserve enough room for the admitted Run to expose a bounded blocked
    // state and cancellation as well as its initial public history event.
    // The bound here covers the owning principal and the SDK's UUID request
    // identity; other long transport identities need their own boundary checks.
    const max = Number.MAX_SAFE_INTEGER, uuid = "00000000-0000-0000-0000-000000000000";
    const binding = { domainID: store.domainID, principalID: principal };
    const envelope = (result: unknown) => ({ protocolVersion: "harness/1", requestID: uuid, binding, ok: true, result });
    const cursor = `${Buffer.from(canonicalJson({ high: max, position: max })).toString("base64url")}.${"x".repeat(43)}`;
    const blockIssue = { code: "LIMIT_EXCEEDED", message: "Driver completion exceeds response limit", details: { frameID: uuid } };
    const blockedView = { ...view, blocked: blockIssue, revision: max, updatedAt: max, rootActionCount: max, frameCount: max };
    const cancelledView = { ...view, lifecycle: "completed", cancelRequested: true, cancellation: { source: "client", reason: null, requestedAt: max },
      outcome: { kind: "cancelled", reason: null }, revision: max, updatedAt: max, completedAt: max, rootActionCount: max, frameCount: max };
    const event = (type: string, data: Json) => ({ id: uuid, sequence: max, at: max, runID, actionID: null, attemptID: null, type, data });
    const accepted = event("run.accepted", json({ principalID: principal, requestKey: key, agent: candidate.ref, limits }));
    const blocked = event("run.blocked", json({ blocked: blockIssue }));
    if (prep.fragmented_response !== 1 && [envelope(blockedView), envelope(cancelledView), envelope({ items: [accepted], nextCursor: cursor }),
      envelope({ items: [blocked], nextCursor: cursor })].some(value => jsonBytes(value) > options.maxResponseBytes)) {
      reject(row, prep, harnessError("LIMIT_EXCEEDED", "Initial Run cannot fit within response limit"), at, { candidate, job });
      return;
    }
    const writes: SQLWrite[] = [write("UPDATE commands SET status=?,result_json=?,updated_at=? WHERE principal_id=? AND request_key=? AND status=? AND fingerprint=?",
      ["applied", { json: json(receipt) }, at, principal, key, "pending", row.fingerprint as string]),
      write("UPDATE prepare_rounds SET state=? WHERE id=? AND state=? AND deadline_at>?", ["accepted", round, "pending", at]),
      write("UPDATE validation_jobs SET state=? WHERE id=? AND round_id=? AND state=? AND assignment_id=? AND assignment_epoch=? AND payload_json=? AND report_json=?", ["accepted", job.id as string, round, "accepted", job.assignment_id as string, job.assignment_epoch as number, job.payload_json as string, job.report_json as string]),
      write("UPDATE submit_preparations SET candidate_json=? WHERE principal_id=? AND request_key=? AND round_id=? AND candidate_json=? AND pointer_revision IS ?", [{ json: json(candidate.ref) }, principal, key, round, prep.candidate_json as string, prep.pointer_revision as number | null])];
    if (candidate.revision !== null) writes.push(write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [candidate.revision, "agent", candidate.ref.id, candidate.ref.version, candidate.revision]));
    writes.push(write("UPDATE work_items SET stage=? WHERE id=? AND stage=?", ["done", identity("submit.work", principal, key), "pending"]));
    writes.push(write("INSERT INTO runs (id,principal_id,revision,epoch,frame_count,snapshot_json) VALUES (?,?,?,?,?,?)", [runID, principal, 0, 0, 0, { json: encodeRunInput(store, view, inputSource) }]));
    if (inputSource) writes.push(write("INSERT INTO root_run_inputs(run_id,principal_id,request_key) VALUES(?,?,?)", [runID, principal, key]));
    writes.push(write("INSERT INTO work_items (id,kind,subject_id,stage,revision,available_at,payload_json,result_json) VALUES (?,?,?,?,?,?,?,?)", [randomUUID(), "run", runID, "start", 0, at, { json: json({ runID }) }, null]));
    commit("submit.applied", principal, key, { request: decode(row.request_json), candidate, conclusion, limits, deadlineAt: deadline, event: "run.accepted" }, writes, receipt, deadline, undefined,
      [{ type: "run.accepted", subjectID: runID, data: json({ principalID: principal, requestKey: key, agent: candidate.ref, limits }) }]);
    wake(principal, key);
  }
  function requestScan() {
    if (!closed && !scanning && !scheduled) scheduled = setImmediate(scanPage);
  }
  async function scanPage() {
    scheduled = undefined;
    if (closed) return;
    scanning = true;
    try {
      store.assertAuthority();
      // The indexed cursor carries only narrow keys. Never materialize the entire pending queue.
      const rows = store.query(`SELECT created_at,principal_id,request_key FROM commands WHERE operation=? AND status=?${cursor ? " AND (created_at,principal_id,request_key)>(?,?,?)" : ""} ORDER BY created_at,principal_id,request_key LIMIT ?`,
        ["submit", "pending", ...(cursor ?? []), scanBatchSize]);
      for (const key of rows) {
        const principal = key.principal_id as string, requestKey = key.request_key as string;
        try {
          const row = command(principal, requestKey);
          if (row?.status === "pending") await process(row);
        } catch (error) {
          if (!(error instanceof HarnessError) || error.info.code !== "REVISION_CONFLICT") throw error;
        } finally {
          // Wake only this command's callers, including for credential revalidation while pending.
          wake(principal, requestKey);
        }
      }
      if (rows.length === scanBatchSize) {
        const last = rows[rows.length - 1]!;
        cursor = [last.created_at as number, last.principal_id as string, last.request_key as string];
        scheduled = setImmediate(scanPage);
        return;
      }
    } catch { /* Failed/unknown storage or fencing never implies rejection. */ wakeAll(); }
    cursor = undefined;
    scanning = false;
  }
  return {
    start() { if (!timer && !closed) { requestScan(); timer = setInterval(requestScan, options.pollMs); timer.unref(); } },
    close() { closed = true; if (timer) clearInterval(timer); if (scheduled) clearImmediate(scheduled); wakeAll(); },
    async submit(request: SubmitRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant = () => principal, requestID = "00000000-0000-0000-0000-000000000000", fragmentedResponses = false): Promise<SubmitReceipt> {
      request = copyJson(request) as unknown as SubmitRequest;
      let registered = false;
      const check = () => {
        let state: CommandState;
        try { state = current(principal.principalID, request, revalidate()); }
        catch (error) {
          // Losing permission or authority ends this call, not the durable pending command.
          // Do not disclose its receipt to a credential that no longer has access.
          if (error instanceof HarnessError && registered && error.info.code !== "REQUEST_CONFLICT") {
            throw new HarnessError({ ...error.info, disposition: "uncertain" }, { cause: error });
          }
          if (error instanceof HarnessError && error.info.code === "INTERNAL_ERROR") {
            throw harnessError("INTERNAL_ERROR", "Command status cannot be confirmed", null, "uncertain");
          }
          throw error;
        }
        // A durable rejected receipt is authoritative, unlike a failed status read.
        if (state.status === "rejected") throw new HarnessError(state.error);
        if (state.status === "pending") registered = true;
        return state.status === "applied" ? state.receipt : null;
      };
      const existing = check();
      if (existing) return existing;
      let exists: Row | undefined;
      try { exists = command(principal.principalID, request.requestKey); }
      catch (error) {
        if (error instanceof HarnessError && error.info.code === "INTERNAL_ERROR") throw harnessError("INTERNAL_ERROR", "Command status cannot be confirmed", null, "uncertain");
        throw error;
      }
      if (!exists) {
        if (!fragmentedResponses && request.agent.version !== undefined) {
          // Even the shortest valid timestamp and a UUID cannot fit: reject
          // before registering a command. Existing receipts are replayed above.
          const smallest = { protocolVersion: "harness/1", requestID, binding: { domainID: store.domainID, principalID: principal.principalID }, ok: true,
            result: { runID: "00000000-0000-0000-0000-000000000000", acceptedAt: 0, agent: { id: request.agent.id, version: request.agent.version } } };
          if (Buffer.byteLength(canonicalJson(smallest), "utf8") > options.maxResponseBytes) throw harnessError("LIMIT_EXCEEDED", "Submit receipt cannot fit in the response limit");
        }
        const envelope = { protocolVersion: "harness/1", requestID, binding: { domainID: store.domainID, principalID: principal.principalID }, ok: true, result: null };
        const responseBudget = fragmentedResponses ? null : options.maxResponseBytes - (Buffer.byteLength(canonicalJson(envelope), "utf8") - 4);
        try { await register(request, principal.principalID, revalidate, responseBudget, fragmentedResponses); registered = true; }
        catch (error) {
          if (error instanceof HarnessError && error.info.code === "INTERNAL_ERROR") {
            throw harnessError("INTERNAL_ERROR", "Command registration cannot be confirmed", null, "uncertain");
          }
          if (!(error instanceof HarnessError) || !["REVISION_CONFLICT", "REQUEST_CONFLICT"].includes(error.info.code)) throw error;
          // A candidate or absent condition changed under the transaction lock.
          // Re-read the identity and retry without extending any existing deadline.
          return this.submit(request, principal, revalidate, requestID, fragmentedResponses);
        }
      }
      requestScan();
      for (;;) {
        if (closed) throw harnessError("TRANSPORT_UNCERTAIN", "Coordinator stopped while submit was pending");
        const receipt = check();
        if (receipt) return receipt;
        await new Promise<void>(resolve => {
          const id = identity("wait", principal.principalID, request.requestKey);
          let waiting = waiters.get(id);
          if (!waiting) waiters.set(id, waiting = new Set());
          waiting.add(resolve);
        });
      }
    },
  };
}
