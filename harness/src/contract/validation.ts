// Shared public protocol validation; business execution belongs to runtime modules.
import { isProxy } from "node:util/types";
import { harnessError } from "./errors.js";
import { copyJson, parseJson } from "./json.js";
import type {
  AdvanceInput, CapacityResult, CommandView, Decision, ErrorInfo, Json, PublicMethod,
  Reconciliation, Report, RPCRequest, RPCResponse, RunView,
} from "./types.js";
import type { NodeMethod, NodeRPCRequest, NodeRPCResponse, ValidationAssignment, ValidationConclusion, ValidationPayload } from "./node.js";

type Schema = (value: unknown, path: string) => unknown;
type Field = Schema | { optional: Schema };
const fail = (path: string, message: string): never => {
  throw harnessError("INVALID_ARGUMENT", `${path}: ${message}`);
};
const json: Schema = value => copyJson(value);
const evidence: Schema = (value, path) => {
  const copied = copyJson(value);
  if (copied === null || copied === false || copied === 0 || (typeof copied === "string" && !copied.trim())
      || (typeof copied === "object" && Object.keys(copied).length === 0)) {
    fail(path, "expected nonempty reviewable evidence");
  }
  return copied;
};
const text: Schema = (value, path) => typeof value === "string" ? value : fail(path, "expected string");
const nonemptyText: Schema = (value, path) => (text(value, path) as string).length > 0 ? value : fail(path, "expected nonempty string");
const nonblankText: Schema = (value, path) => (text(value, path) as string).trim().length > 0 ? value : fail(path, "expected nonblank text");
const nonempty: Schema = (value, path) => {
  const string = text(value, path) as string;
  return string.length && string.trim() === string ? string : fail(path, "expected nonempty unpadded string");
};
const bool: Schema = (value, path) => typeof value === "boolean" ? value : fail(path, "expected boolean");
const number: Schema = (value, path) => typeof value === "number" ? copyJson(value) : fail(path, "expected number");
const natural: Schema = (value, path) => typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : fail(path, "expected nonnegative safe integer");
const positive: Schema = (value, path) => (natural(value, path) as number) > 0 ? value : fail(path, "expected positive safe integer");
const duration: Schema = natural;
const optional = (schema: Schema): Field => ({ optional: schema });
const nullable = (schema: Schema): Schema => (value, path) => value === null ? null : schema(value, path);
const oneOf = (...allowed: readonly (string | boolean | number)[]): Schema => (value, path) => allowed.includes(value as string) ? value : fail(path, "unexpected value");
const list = (schema: Schema): Schema => (value, path) => {
  if (!Array.isArray(value) || isProxy(value) || Object.getPrototypeOf(value) !== Array.prototype) fail(path, "expected plain array");
  const array = value as unknown[];
  const descriptors = Object.getOwnPropertyDescriptors(array);
  if (Reflect.ownKeys(descriptors).length !== array.length + 1) fail(path, "expected dense unextended array");
  const result: unknown[] = [];
  for (let index = 0; index < array.length; index++) {
    const descriptor = descriptors[String(index)];
    if (!descriptor || !("value" in descriptor) || !descriptor.enumerable) fail(path, "expected data elements");
    result.push(schema(descriptor!.value, `${path}[${index}]`));
  }
  return result;
};
function object(fields: Record<string, Field>): Schema {
  return (value, path) => {
    if (!value || typeof value !== "object" || Array.isArray(value) || isProxy(value)) fail(path, "expected plain object");
    const proto = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) fail(path, "expected plain object");
    const descriptors = Object.getOwnPropertyDescriptors(value);
    for (const key of Reflect.ownKeys(descriptors)) {
      if (typeof key !== "string" || !Object.hasOwn(fields, key)) fail(path, "unknown field");
      const descriptor = descriptors[key as string]!;
      if (!("value" in descriptor) || !descriptor.enumerable) fail(path, "expected data properties");
    }
    const result: Record<string, unknown> = {};
    for (const [key, field] of Object.entries(fields)) {
      const descriptor = descriptors[key];
      const isOptional = typeof field !== "function";
      if (!descriptor || (isOptional && descriptor.value === undefined)) {
        if (isOptional) continue;
        fail(`${path}.${key}`, "missing field");
      }
      const schema = isOptional ? field.optional : field;
      result[key] = schema(descriptor.value, `${path}.${key}`);
    }
    return result;
  };
}
function tagged(tag: string, variants: Record<string, Schema>): Schema {
  return (value, path) => {
    if (!value || typeof value !== "object" || isProxy(value)) fail(path, "expected tagged object");
    const descriptor = Object.getOwnPropertyDescriptor(value, tag);
    if (!descriptor || !("value" in descriptor) || typeof descriptor.value !== "string" || !Object.hasOwn(variants, descriptor.value)) fail(path, "unknown variant");
    return variants[descriptor!.value as string]!(value, path);
  };
}

const ref = object({ id: nonempty, version: nonempty });
const selector = object({ id: nonempty, version: optional(nonempty) });
const issue = object({ code: nonempty, message: text, details: json });
const continuation = object({ entry: text, state: json });
const capacityResult = tagged("kind", {
  succeeded: object({ kind: oneOf("succeeded"), value: json }),
  failed: object({ kind: oneOf("failed"), error: issue }),
});
const outcome = tagged("kind", {
  succeeded: object({ kind: oneOf("succeeded"), value: json }),
  failed: object({ kind: oneOf("failed"), error: issue }),
  cancelled: object({ kind: oneOf("cancelled"), reason: nullable(text) }),
});
const metric = object({ name: nonemptyText, value: number, unit: nullable(nonemptyText) });
const limitsFields = { maxDepth: positive, maxActions: positive, maxFrames: positive, maxExecutionsPerAction: positive, defaultWaitTimeoutMs: nullable(duration) };
const limits = object(limitsFields);
const partialLimits = object(Object.fromEntries(Object.entries(limitsFields).map(([key, schema]) => [key, optional(schema)])));
const kind = oneOf("agent", "capacity");
const policy = object({ ref, config: json });
const actionRequest = object({ key: nonempty, kind, target: selector, input: json, config: json });
const preparedRequest = object({ key: nonempty, kind, target: ref, input: json, config: json, policies: list(policy) });
const cancellation = object({ source: oneOf("client", "parent", "completion", "agent"), reason: nullable(text), requestedAt: natural });
const errorCode = oneOf("INVALID_ARGUMENT", "UNAUTHENTICATED", "FORBIDDEN", "PROTOCOL_MISMATCH", "SESSION_MISMATCH", "NOT_FOUND", "REQUEST_CONFLICT", "REVISION_CONFLICT", "STATE_CONFLICT", "VERSION_UNAVAILABLE", "DUPLICATE_DEFINITION", "LIMIT_EXCEEDED", "RUN_TERMINAL", "ACTION_TERMINAL", "EXECUTION_FENCED", "RECONCILIATION_REQUIRED", "CHECKPOINT_BUSY", "NODE_STATE_INVALID", "INVALID_DECISION", "OUTPUT_VALIDATION_FAILED", "CALL_ABORTED", "TRANSPORT_UNCERTAIN", "INTERNAL_ERROR");
const errorInfo: Schema = (value, path) => {
  const info = object({ code: errorCode, message: text, disposition: oneOf("rejected", "uncertain"), details: json, retryAfterMs: nullable(duration) })(value, path) as ErrorInfo;
  if (info.code === "TRANSPORT_UNCERTAIN" && info.disposition !== "uncertain") fail(path, "transport uncertainty cannot be a rejection");
  return info;
};
const checkpoint = object({ revision: natural, value: json });
const attempt = object({ id: nonempty, number: positive, nodeID: nonempty, recovering: bool, mode: oneOf("automatic-recovery", "recheck-only"), executeDispatched: bool, startedAt: natural, finishedAt: nullable(natural) });
const observation = object({ id: nonempty, attemptID: nullable(nonempty), source: oneOf("execute", "reconcile", "operator", "remote"), observedAt: natural, result: nullable(outcome), evidence: json, validationError: nullable(issue), measurements: list(metric), reported: json, validated: bool, acceptedAsOutcome: bool });
const actionView = object({ id: nonempty, runID: nonempty, revision: natural, original: actionRequest, request: preparedRequest, status: oneOf("accepted", "running", "settled"), latestAttempt: nullable(attempt), executionCount: natural, checkpoint, outcome: nullable(outcome), outcomeObservationID: nullable(nonempty), cancelRequested: bool, cancellation: nullable(cancellation), blocked: nullable(issue), recheckRequested: bool, remoteRunID: nullable(nonempty), latestObservation: nullable(observation), createdAt: natural, updatedAt: natural, settledAt: nullable(natural) });
const wait = object({ id: nonempty, actionKeys: list(nonempty), deadlineAt: nullable(natural), winner: nullable(oneOf("results", "timeout")) });
const runViewShape = object({ id: nonempty, rootID: nonempty, parentActionID: nullable(nonempty), revision: natural, agent: ref, input: json, config: json, lifecycle: nullable(oneOf("started", "suspended", "resumed", "completed")), continuation, wait: nullable(wait), outcome: nullable(outcome), cancelRequested: bool, cancellation: nullable(cancellation), blocked: nullable(issue), limits, rootActionCount: natural, frameCount: natural, createdAt: natural, updatedAt: natural, completedAt: nullable(natural) });
const runView: Schema = (value, path) => {
  const run = runViewShape(value, path) as RunView;
  const completed = run.lifecycle === "completed";
  if (completed ? run.outcome === null || run.completedAt === null : run.outcome !== null || run.completedAt !== null) {
    fail(path, "inconsistent Run lifecycle, outcome and completion time");
  }
  return run;
};
const versionPointer = object({ kind, id: nonempty, ref: nullable(ref), revision: natural, evidence: json, updatedAt: nullable(natural) });
const submitReceipt = object({ runID: nonempty, acceptedAt: natural, agent: ref });
const commandResults: Record<CommandView["operation"], Schema> = {
  submit: submitReceipt,
  cancel: runView,
  resumeRun: runView,
  recheckAction: actionView,
  resolveAction: actionView,
  adoptVersion: versionPointer,
};
const command: Schema = (value, path) => {
  const result = object({ requestKey: nonempty, operation: oneOf("submit", "cancel", "resumeRun", "recheckAction", "resolveAction", "adoptVersion"), status: oneOf("pending", "applied", "rejected"), result: nullable(json), error: nullable(errorInfo), createdAt: natural, updatedAt: natural })(value, path) as CommandView;
  if (result.status === "pending" && (result.result !== null || result.error !== null)
      || result.status === "applied" && result.error !== null
      || result.status === "rejected" && (result.result !== null || result.error === null || result.error.disposition !== "rejected")) {
    fail(path, "inconsistent command status and receipt");
  }
  if (result.status === "applied") result.result = commandResults[result.operation](result.result, `${path}.result`) as Json;
  return result;
};
const history = object({ id: nonempty, sequence: positive, at: natural, runID: nullable(nonempty), actionID: nullable(nonempty), attemptID: nullable(nonempty), type: nonempty, data: json });
const binding = object({ domainID: nonempty, principalID: nonempty });
const domainInfo = object({ domainID: nonempty, principalID: nonempty, protocolVersion: oneOf("harness/1"), defaultLimits: limits, maxPayloadBytes: positive, maxValueBytes: positive, maxFrameBytes: positive, maxResponseBytes: positive, maxPageSize: positive, idempotencyRetention: oneOf("until-domain-reset") });
const page = (item: Schema) => object({ items: list(item), nextCursor: nullable(nonempty) });
const pagination = { cursor: optional(nonempty), limit: optional(positive) };
const resolution = tagged("kind", { settle: object({ kind: oneOf("settle"), result: capacityResult, measurements: list(metric) }), allow_retry: object({ kind: oneOf("allow_retry") }),
  allow_model_retry: object({ kind: oneOf("allow_model_retry"), acceptDuplicateCharges: oneOf(true) }),
  legacy_report: object({ kind: oneOf("legacy_report"), reportID: nonempty, attemptID: nonempty, phase: oneOf("execute", "reconcile"), reportDigest: nonempty,
    maxValueBytes: positive, basis: oneOf("recorded_configuration", "administrator_approval"), source: nonblankText, scope: nonblankText }) });
const subject = tagged("kind", { run: object({ kind: oneOf("run"), runID: nonempty }), action: object({ kind: oneOf("action"), actionID: nonempty }), version: object({ kind: oneOf("version"), definitionKind: kind, id: nonempty }) });
const methods: Record<PublicMethod, { params: Schema; result: Schema }> = {
  "session.getInfo": { params: object({}), result: domainInfo },
  "client.submit": { params: object({ requestKey: nonempty, agent: selector, input: json, config: json, limits: optional(partialLimits) }), result: submitReceipt },
  "client.getRun": { params: object({ runID: nonempty }), result: runView },
  "client.getRunByRequestKey": { params: object({ requestKey: nonempty }), result: nullable(runView) },
  "client.getAction": { params: object({ actionID: nonempty }), result: actionView },
  "client.listActions": { params: object({ runID: nonempty, ...pagination }), result: page(actionView) },
  "client.requestCancel": { params: object({ requestKey: nonempty, runID: nonempty, reason: nullable(text) }), result: runView },
  "client.getCommand": { params: object({ requestKey: nonempty }), result: nullable(command) },
  "client.getVersion": { params: object({ kind, id: nonempty }), result: versionPointer },
  "client.getHistory": { params: object({ subject, ...pagination }), result: page(history) },
  "admin.resumeRun": { params: object({ requestKey: nonempty, runID: nonempty, expectedRevision: natural, evidence }), result: runView },
  "admin.recheckAction": { params: object({ requestKey: nonempty, actionID: nonempty, expectedRevision: natural, reason: nonblankText }), result: actionView },
  "admin.resolveAction": { params: object({ requestKey: nonempty, actionID: nonempty, expectedRevision: natural, resolution, evidence }), result: actionView },
  "admin.adoptVersion": { params: object({ requestKey: nonempty, kind, ref, expectedRevision: natural, evidence }), result: versionPointer },
};

function method(value: unknown): PublicMethod {
  if (typeof value !== "string" || !Object.hasOwn(methods, value)) fail("request.method", "unknown public method");
  return value as PublicMethod;
}

/** SDK boundary: optional undefined is omitted; Json extension values remain strict. */
export function validateRPCRequest(value: unknown): RPCRequest {
  if (!value || typeof value !== "object" || isProxy(value)) fail("request", "expected object");
  const selected = method(Object.getOwnPropertyDescriptor(value, "method")?.value);
  const request = object({ protocolVersion: text, requestID: nonempty, binding: selected === "session.getInfo" ? nullable(binding) : binding, method: oneOf(selected), params: methods[selected].params })(value, "request") as RPCRequest;
  if (request.protocolVersion !== "harness/1") throw harnessError("PROTOCOL_MISMATCH", "Unsupported Harness protocol");
  return request;
}

export function parseRPCRequest(source: string): RPCRequest {
  return validateRPCRequest(parseJson(source));
}

export function validateRPCResponse<M extends PublicMethod>(value: unknown, expectedMethod: M): RPCResponse<M> {
  method(expectedMethod);
  if (!value || typeof value !== "object" || isProxy(value)) fail("response", "expected object");
  const ok = Object.getOwnPropertyDescriptor(value, "ok")?.value;
  const shape = ok === true
    ? object({ protocolVersion: oneOf("harness/1"), requestID: nonempty, binding, ok: oneOf(true), result: methods[expectedMethod].result })
    : object({ protocolVersion: oneOf("harness/1"), requestID: nullable(nonempty), binding: nullable(binding), ok: oneOf(false), error: errorInfo });
  return shape(value, "response") as RPCResponse<M>;
}

export function validateErrorInfo(value: unknown): ErrorInfo {
  return errorInfo(value, "error") as ErrorInfo;
}

const nodeIdentity = { nodeID: nonempty, incarnation: nonempty };
const definitionDescriptor = object({ kind: oneOf("agent", "capacity", "policy"), ref, supportsReconcile: optional(bool) });
const validationPayload = object({ kind, ref, phase: oneOf("input", "output", "rewrite", "admission", "report-output"), value: json, config: json, principalID: nonempty,
  operation: (value: unknown) => method(value), context: json });
const validationConclusion: Schema = (value, path) => {
  const result = object({ valid: bool, issue: nullable(issue), rewritten: optional(list(actionRequest)) })(value, path) as ValidationConclusion;
  if (result.valid !== (result.issue === null)) fail(path, "inconsistent validation conclusion");
  if (!result.valid && result.rewritten !== undefined) fail(path, "invalid rewrite conclusion");
  return result;
};
const validationAssignment: Schema = (value, path) => {
  const result = object({ jobID: nonempty, roundID: nonempty, payload: validationPayload, assignmentID: nonempty, assignmentEpoch: positive,
    ...nodeIdentity, deadlineAt: natural, roundDeadlineAt: natural })(value, path) as ValidationAssignment;
  if (result.deadlineAt > result.roundDeadlineAt) fail(path, "assignment exceeds preparation deadline");
  return result;
};
const businessLease = object({ kind: oneOf("run", "action"), subjectID: nonempty, ...nodeIdentity, workerPrincipalID: nonempty,
  epoch: positive, expiresAt: natural, attemptID: nullable(nonempty), attemptNumber: nullable(positive) });
const driverAssignment = object({ driver: oneOf(true), jobID: nonempty, runID: nonempty, principalID: nonempty,
  input: value => validateAdvanceInput(value), assignmentID: nonempty, assignmentEpoch: positive, ...nodeIdentity, deadlineAt: natural,
  lease: businessLease, leaseTTLms: positive });
const finalizeAssignment = object({ finalize: oneOf(true), jobID: nonempty, runID: nonempty, principalID: nonempty,
  input: value => validateAdvanceInput(value), assignmentID: nonempty, assignmentEpoch: positive, ...nodeIdentity, deadlineAt: natural,
  lease: businessLease, leaseTTLms: positive });
const startAssignment = object({ start: oneOf(true), jobID: nonempty, runID: nonempty, principalID: nonempty, agent: ref,
  assignmentID: nonempty, assignmentEpoch: positive, ...nodeIdentity, deadlineAt: natural, lease: businessLease, leaseTTLms: positive });
const wakeAssignment = object({ wake: oneOf(true), jobID: nonempty, waitID: nonempty, runID: nonempty, principalID: nonempty, agent: ref,
  assignmentID: nonempty, assignmentEpoch: positive, ...nodeIdentity, deadlineAt: natural, lease: businessLease, leaseTTLms: positive });
const actionAssignment = object({ action: oneOf(true), runID: nonempty, rootID: nonempty, actionID: nonempty, principalID: nonempty,
  request: preparedRequest, checkpoint, mode: oneOf("automatic-recovery", "recheck-only"), recovering: bool, stage: oneOf("execute", "reconcile"),
  attemptID: nonempty, attemptNumber: positive, assignmentID: nonempty, ...nodeIdentity, lease: businessLease, leaseTTLms: positive, reportValueLimit: optional(positive) });
const nodeAssignment: Schema = (value, path) => value && typeof value === "object" && Object.getOwnPropertyDescriptor(value, "driver")?.value === true
  ? driverAssignment(value, path) : value && typeof value === "object" && Object.getOwnPropertyDescriptor(value, "finalize")?.value === true
    ? finalizeAssignment(value, path) : value && typeof value === "object" && Object.getOwnPropertyDescriptor(value, "start")?.value === true
      ? startAssignment(value, path) : value && typeof value === "object" && Object.getOwnPropertyDescriptor(value, "wake")?.value === true
        ? wakeAssignment(value, path) : value && typeof value === "object" && Object.getOwnPropertyDescriptor(value, "action")?.value === true
        ? actionAssignment(value, path) : validationAssignment(value, path);
const driverConclusion: Schema = (value, path) => {
  if (!value || typeof value !== "object") return fail(path, "expected conclusion");
  return Object.getOwnPropertyDescriptor(value, "ok")?.value === true && Object.getOwnPropertyDescriptor(value, "woken")?.value === true
    ? object({ ok: oneOf(true), woken: oneOf(true) })(value, path) : Object.getOwnPropertyDescriptor(value, "ok")?.value === true && Object.getOwnPropertyDescriptor(value, "started")?.value === true
    ? object({ ok: oneOf(true), started: oneOf(true) })(value, path) : Object.getOwnPropertyDescriptor(value, "ok")?.value === true && Object.getOwnPropertyDescriptor(value, "finalized")?.value === true
    ? object({ ok: oneOf(true), finalized: oneOf(true) })(value, path) : Object.getOwnPropertyDescriptor(value, "ok")?.value === true
    ? object({ ok: oneOf(true), decision: json })(value, path)
    : object({ ok: oneOf(false), issue })(value, path);
};
const nodeConclusion: Schema = (value, path) => value && typeof value === "object" && Object.hasOwn(value, "ok")
  ? driverConclusion(value, path) : validationConclusion(value, path);
const nodeSession = object({ ...nodeIdentity, expiresAt: natural, serverTime: natural, heartbeatMs: positive, pollMs: positive });
const nodeMethods: Record<NodeMethod, { params: Schema; result: Schema }> = {
  register: { params: object({ ...nodeIdentity, definitions: list(definitionDescriptor), concurrency: object({ runs: positive, actions: positive }), policies: list(policy) }), result: nodeSession },
  renew: { params: object({ ...nodeIdentity, renewRequestID: nonempty }), result: nodeSession },
  quiesce: { params: object({ ...nodeIdentity, pendingClaimRequestID: optional(nonempty) }), result: object({ quiesced: oneOf(true) }) },
  claim: { params: object({ ...nodeIdentity, claimRequestID: nonempty, validationAvailable: optional(bool), runAvailable: optional(bool), actionAvailable: optional(bool) }), result: object({ assignment: nullable(nodeAssignment) }) },
  report: { params: object({ ...nodeIdentity, assignment: nodeAssignment, conclusion: nodeConclusion, grant: optional(businessLease) }), result: object({ conclusion: nodeConclusion }) },
  renewLease: { params: object({ ...nodeIdentity, grant: businessLease, renewRequestID: nonempty }), result: object({ grant: businessLease, ttlMs: positive }) },
  verifyLease: { params: object({ ...nodeIdentity, grant: businessLease }), result: object({ valid: oneOf(true) }) },
  agentExecution: { params: object({ ...nodeIdentity, grant: businessLease }), result: object({ runID: nonempty, rootID: nonempty, principalID: nonempty, actionRunID: nullable(nonempty), request: nullable(preparedRequest) }) },
  verifyValidation: { params: object({ ...nodeIdentity, assignment: validationAssignment }), result: object({ valid: oneOf(true) }) },
  readValidationValue: { params: object({ ...nodeIdentity, assignment: validationAssignment, offset: natural, maxBytes: positive, source: optional(oneOf("policy", "report", "action", "run-config")) }), result: object({ offset: natural, data: nonemptyText, done: bool }) },
  releaseLease: { params: object({ ...nodeIdentity, grant: businessLease, releaseRequestID: nonempty }), result: object({ released: oneOf(true) }) },
  dispatchAction: { params: object({ ...nodeIdentity, grant: businessLease, actionID: nonempty, attemptID: nonempty, dispatchRequestID: nonempty }), result: object({ executionCount: natural }) },
  actionContinuation: { params: object({ ...nodeIdentity, grant: businessLease, actionID: nonempty, attemptID: nonempty }), result: object({ state: oneOf("pending", "execute", "stop") }) },
  checkpointAction: { params: object({ ...nodeIdentity, grant: businessLease, actionID: nonempty, attemptID: nonempty, operationID: nonempty, expectedRevision: natural, value: json }), result: object({ checkpoint }) },
  readCheckpoint: { params: object({ ...nodeIdentity, grant: businessLease, actionID: nonempty, attemptID: nonempty, operationID: nonempty }), result: object({ checkpoint: nullable(checkpoint) }) },
  reportAction: { params: object({ ...nodeIdentity, grant: businessLease, actionID: nonempty, attemptID: nonempty, assignmentID: nonempty, phase: oneOf("execute", "reconcile"), observationOnly: optional(bool), report: tagged("kind", { report: object({ kind: oneOf("report"), value: json }), diagnostic: object({ kind: oneOf("diagnostic"), code: nonempty, phase: oneOf("execute", "reconcile"), digest: optional(nonempty) }) }) }), result: object({ stored: oneOf(true) }) },
  diagnoseAction: { params: object({ ...nodeIdentity, grant: businessLease, actionID: nonempty, attemptID: nonempty, phase: oneOf("execute", "reconcile"), diagnosticID: nonempty, code: nonempty }), result: object({ stored: oneOf(true) }) },
  unregister: { params: object(nodeIdentity), result: object({ stopped: oneOf(true) }) },
};
function nodeMethod(value: unknown): NodeMethod {
  if (typeof value !== "string" || !Object.hasOwn(nodeMethods, value)) fail("request.method", "unknown node method");
  return value as NodeMethod;
}
export function validateNodeRequest(value: unknown): NodeRPCRequest {
  if (!value || typeof value !== "object" || isProxy(value)) fail("request", "expected object");
  const selected = nodeMethod(Object.getOwnPropertyDescriptor(value, "method")?.value);
  const request = object({ protocolVersion: text, requestID: nonempty, binding, method: oneOf(selected), params: nodeMethods[selected].params })(value, "request") as NodeRPCRequest;
  if (request.protocolVersion !== "harness/1") throw harnessError("PROTOCOL_MISMATCH", "Unsupported Harness protocol");
  return request;
}
export function validateNodeResponse<M extends NodeMethod>(value: unknown, expectedMethod: M): NodeRPCResponse<M> {
  nodeMethod(expectedMethod);
  if (!value || typeof value !== "object" || isProxy(value)) fail("response", "expected object");
  const ok = Object.getOwnPropertyDescriptor(value, "ok")?.value;
  return (ok === true
    ? object({ protocolVersion: oneOf("harness/1"), requestID: nonempty, binding, ok: oneOf(true), result: nodeMethods[expectedMethod].result })
    : object({ protocolVersion: oneOf("harness/1"), requestID: nullable(nonempty), binding: nullable(binding), ok: oneOf(false), error: errorInfo }))(value, "response") as NodeRPCResponse<M>;
}
export function validateValidationPayload(value: unknown): ValidationPayload { return validationPayload(value, "validation.payload") as ValidationPayload; }
export function validateActionRequest(value: unknown): import("./types.js").ActionRequest { return actionRequest(value, "action") as import("./types.js").ActionRequest; }
export function validatePreparedActionRequest(value: unknown): import("./types.js").PreparedActionRequest { return preparedRequest(value, "action") as import("./types.js").PreparedActionRequest; }
export function validateValidationAssignment(value: unknown): ValidationAssignment { return validationAssignment(value, "validation.assignment") as ValidationAssignment; }

const fact = object({ id: nonempty, actionID: nonempty, actionKey: nonempty, outcome });
const wake = tagged("kind", { start: object({ kind: oneOf("start") }), results: object({ kind: oneOf("results"), waitID: nonempty, facts: list(fact) }), timeout: object({ kind: oneOf("timeout"), waitID: nonempty, facts: list(fact), pendingActionKeys: list(nonempty) }) });
export function validateAdvanceInput(value: unknown): AdvanceInput {
  return object({ frameID: nonempty, run: object({ id: nonempty, rootID: nonempty, parentActionID: nullable(nonempty), agent: ref, input: json, config: json, limits }), continuation, wake })(value, "frame") as AdvanceInput;
}

export function validateDecision(value: unknown): Decision {
  return tagged("kind", {
    dispatch: object({ kind: oneOf("dispatch"), continuation, actions: list(actionRequest), wait: object({ all: list(nonempty), timeoutMs: optional(nullable(duration)) }) }),
    complete: object({ kind: oneOf("complete"), outcome, remainingActions: oneOf("cancel") }),
  })(value, "decision") as Decision;
}

const reconciliation = tagged("kind", { settled: object({ kind: oneOf("settled"), result: capacityResult }), retryable: object({ kind: oneOf("retryable") }), unresolved: object({ kind: oneOf("unresolved"), reason: issue }) });
export function validateReport(value: unknown, phase: "execute"): Report<CapacityResult>;
export function validateReport(value: unknown, phase: "reconcile"): Report<Reconciliation>;
export function validateReport(value: unknown, phase: "execute" | "reconcile"): Report<CapacityResult | Reconciliation> {
  if (phase !== "execute" && phase !== "reconcile") fail("report.phase", "unknown report phase");
  const report = object({ payload: phase === "execute" ? capacityResult : reconciliation, evidence: json, measurements: list(metric) })(value, "report") as Report<CapacityResult | Reconciliation>;
  if (phase === "reconcile" && (report.payload.kind === "settled" || report.payload.kind === "retryable")) evidence(report.evidence, "report.evidence");
  return report;
}
