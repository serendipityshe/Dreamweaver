import { createHash } from "node:crypto";
import { canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, jsonEqual, parseJson, type Decision, type Json, type RunView, type ValidationPayload } from "../contract/index.js";
import type { SQLiteStore, ValuePublication } from "./sqlite.js";
import { driverValueSource, readFrameDecision } from "./driver-value.js";
import { rootInputBinding } from "./submit-input.js";
import { childInputBinding } from "./child-input.js";
import { actionRequestValueSource } from "./fixed-action-request.js";

type ObjectJson = { [key: string]: Json };
export interface RunConfigValidationSource { encoding: "run-config-source-v1"; runID: string; frameID: string; principalID: string; agent: { id: string; version: string }; inputSource: Json; value: ValuePublication }

/** Follow only the Run's already committed input binding. Root command and
 * parent Action readers authenticate the original publication evidence. */
export function runConfigValidationSource(store: SQLiteStore, runID: string, frameID: string, principalID: string, agent: { id: string; version: string }): RunConfigValidationSource | null {
  const row = store.query("SELECT r.principal_id,r.snapshot_json FROM frames f JOIN runs r ON r.id=f.run_id WHERE f.id=? AND r.id=?", [frameID, runID])[0];
  if (!row || row.principal_id !== principalID) throw harnessError("INTERNAL_ERROR", "Run config validation identity is unavailable");
  const run = object(parseJson(row.snapshot_json as string));
  if (run.id !== runID || !sameContent(run.agent, agent)) throw harnessError("INTERNAL_ERROR", "Run config validation Agent changed");
  const root = rootInputBinding(store, run), child = root ? null : childInputBinding(store, run);
  const value = root?.value ?? (child ? actionRequestValueSource(store, child.actionID, child.requestJSON)?.value : null);
  if (!value) return null;
  return { encoding: "run-config-source-v1", runID, frameID, principalID, agent: { ...agent }, inputSource: copyJson(run.inputSource), value };
}
/** Validators receive value only. Large internal decision contexts bind the
 * same immutable candidate by digest instead of embedding output twice. */
export function agentOutputValidation(store: SQLiteStore, run: RunView, frameID: string, principalID: string, decision: Extract<Decision, { kind: "complete" }>, previous?: Json): ValidationPayload {
  if (decision.outcome.kind !== "succeeded") throw harnessError("INTERNAL_ERROR", "Only successful output requires validation");
  let compact = false;
  if (previous !== undefined) {
    const context = object(previous).context;
    compact = !!context && typeof context === "object" && !Array.isArray(context) && Object.hasOwn(context, "decisionDigest");
  } else {
    let bytes = 0;
    for (const part of canonicalJsonChunks(decision)) { bytes += part.length; if (bytes > 16_384) { compact = true; break; } }
  }
  const previousContext = previous === undefined ? null : object(previous).context as ObjectJson;
  const driver = store.query("SELECT report_json FROM driver_jobs WHERE id=?", [frameID])[0];
  const source = previous === undefined ? driverValueSource(driver?.report_json) : driverValueSource(previousContext?.driverSource);
  if (source && (source.frameID !== frameID || !sameContent(source, driverValueSource(driver?.report_json))))
    throw harnessError("INTERNAL_ERROR", "Output validation Driver source changed");
  let largeConfig = false;
  if (previous === undefined) { let length = 0; for (const chunk of canonicalJsonChunks(run.config)) { length += chunk.length; if (length > 16384) { largeConfig = true; break; } } }
  const runConfigSource = largeConfig || previousContext?.runConfigSource !== undefined ? runConfigValidationSource(store, run.id, frameID, principalID, run.agent) : null;
  if (previousContext?.runConfigSource !== undefined && !sameContent(previousContext.runConfigSource, runConfigSource)
      || runConfigSource && canonicalJsonDigest({ input: run.input, config: run.config }) !== runConfigSource.value.digest)
    throw harnessError("INTERNAL_ERROR", "Run config validation source changed");
  return { kind: "agent", ref: run.agent, phase: "output", value: source ? null : decision.outcome.value, config: runConfigSource ? null : run.config, principalID,
    operation: "client.submit", context: copyJson({ runID: run.id, frameID, ...(source ? { driverSource: source } : {}), ...(runConfigSource ? { runConfigSource } : {}),
      ...(compact || source ? { decisionDigest: canonicalJsonDigest(decision) } : { decision }) }) };
}
function object(value: Json): ObjectJson {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw harnessError("INTERNAL_ERROR", "Invalid completion reference");
  return value;
}
function witness(value: Json): Json {
  const hash = createHash("sha256"); let length = 0;
  for (const part of canonicalJsonChunks(value)) { hash.update(part); length += part.length; }
  if (!Number.isSafeInteger(length)) throw harnessError("INTERNAL_ERROR", "Completion source is too large");
  return { length, digest: hash.digest("hex") };
}
function sameContent(left: unknown, right: unknown): boolean {
  const a = canonicalJsonChunks(left)[Symbol.iterator](), b = canonicalJsonChunks(right)[Symbol.iterator]();
  for (;;) {
    const x = a.next(), y = b.next();
    if (x.done || y.done) return x.done === y.done;
    if (!Buffer.from(x.value).equals(Buffer.from(y.value))) return false;
  }
}
function sources(store: SQLiteStore, runID: string, frameID: string, jobID: Json | undefined) {
  const row = store.query("SELECT c.decision_json FROM frame_candidates c JOIN frames f ON f.id=c.frame_id WHERE c.frame_id=? AND f.run_id=?", [frameID, runID])[0];
  if (!row) throw harnessError("INTERNAL_ERROR", "Completion decision source is unavailable");
  const decision = copyJson(readFrameDecision(store, frameID, row.decision_json));
  let payload: Json = null;
  if (jobID !== undefined) {
    if (typeof jobID !== "string") throw harnessError("INTERNAL_ERROR", "Invalid completion validation source");
    const job = store.query(`SELECT j.payload_json FROM validation_jobs j JOIN prepare_rounds r ON r.id=j.round_id
      WHERE j.id=? AND r.owner_kind='frame' AND r.owner_id=? AND r.phase='output' AND j.state='accepted'`, [jobID, frameID])[0];
    if (!job) throw harnessError("INTERNAL_ERROR", "Completion validation source is unavailable");
    payload = parseJson(job.payload_json as string);
  }
  return { decision, payload };
}

/** Existing immutable candidate and accepted validation are the payload
 * sources. Core still commits its ordinary proposal, receipt and all effects
 * atomically; it neither stages values nor publishes a separate transaction. */
export function completionRequest(store: SQLiteStore, runID: string, input: unknown): Json {
  const request = object(copyJson(input));
  let bytes = 0;
  for (const part of canonicalJsonChunks(request)) { bytes += part.length; if (bytes > 16_384) break; }
  if (bytes <= 16_384) return request;
  const frameID = request.frameID;
  if (typeof frameID !== "string") throw harnessError("INTERNAL_ERROR", "Completion frame is missing");
  const validation = request.validation === null ? null : object(request.validation!);
  const source = sources(store, runID, frameID, validation?.jobID);
  if (!sameContent(source.decision, request.decision) || validation && !sameContent(source.payload, validation.payload))
    throw harnessError("INTERNAL_ERROR", "Completion source differs from the proposed decision");
  let remote = request.remote ?? null;
  if (remote !== null) {
    const original = object(remote), observation = object(original.observation!), reported = object(observation.reported!), outcome = object(source.decision).outcome!;
    if (original.childRunID !== runID || !sameContent(original.outcome, outcome) || !sameContent(observation.result, outcome) || !sameContent(reported.payload, outcome))
      throw harnessError("INTERNAL_ERROR", "Remote completion result differs from its decision");
    remote = { ...original, encoding: "remote-completion-ref-v1", outcome: null,
      observation: { ...observation, result: null, reported: { ...reported, payload: null } } };
  }
  return { ...request, encoding: "core-completion-ref-v1", decision: null,
    remote,
    validation: validation ? { ...validation, payload: null } : null,
    sources: { decision: witness(source.decision), validation: validation ? witness(source.payload) : null } };
}

/** The outer Core receipt is verified by the caller as before. A digest alone
 * never authorizes a transition: materialize and verify each retained source,
 * then let the existing frame, validation, lease and cancellation checks run. */
export function readCompletionRequest<T>(store: SQLiteStore, runID: string, encoded: unknown): T {
  const request = object(parseJson(encoded as string));
  if (request.encoding !== "core-completion-ref-v1") return request as unknown as T;
  if (typeof request.frameID !== "string" || request.decision !== null) throw harnessError("INTERNAL_ERROR", "Invalid completion source identity");
  const validation = request.validation === null ? null : object(request.validation!);
  if (validation && validation.payload !== null) throw harnessError("INTERNAL_ERROR", "Completion payload slot is not empty");
  const expected = object(request.sources!), source = sources(store, runID, request.frameID, validation?.jobID);
  if (!jsonEqual(expected, { decision: witness(source.decision), validation: validation ? witness(source.payload) : null }))
    throw harnessError("INTERNAL_ERROR", "Completion source content changed");
  const { encoding: _encoding, sources: _sources, ...body } = request;
  let remote = body.remote ?? null;
  if (remote !== null && object(remote).encoding === "remote-completion-ref-v1") {
    const original = object(remote), observation = object(original.observation!), reported = object(observation.reported!);
    if (original.childRunID !== runID || original.outcome !== null || observation.result !== null || reported.payload !== null)
      throw harnessError("INTERNAL_ERROR", "Invalid remote completion slots");
    const { encoding: _remoteEncoding, ...rest } = original, outcome = object(source.decision).outcome!;
    remote = { ...rest, outcome, observation: { ...observation, result: outcome, reported: { ...reported, payload: outcome } } };
  }
  return { ...body, remote, decision: source.decision, validation: validation ? { ...validation, payload: source.payload } : null } as T;
}
