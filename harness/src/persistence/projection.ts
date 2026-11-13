import { createHash } from "node:crypto";
import { canonicalJsonChunks, copyJson, harnessError, parseJson, validateReport, type ActionReport, type Json, type Outcome } from "../contract/index.js";
import { readActionReport, readActionReportForEvidence, readActionReportSync, actionReportValueReference, verifyInlineReportReceipt } from "./report-value.js";
import type { SQLiteStore } from "./sqlite.js";
import type { SQLWrite } from "./writes.js";
import { readFrameResult } from "./frame-result.js";
import { readCommandProjection, readCommandProjectionSync } from "./command-value.js";

export type ReportProjectionSelector = "report" | "result" | "value" | "evidence" | "measurements";
export interface ReportProjectionBinding { reportID: string; path: (string | number)[]; selector: ReportProjectionSelector }
const selectors = new Set<ReportProjectionSelector>(["report", "result", "value", "evidence", "measurements"]);
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid persisted report projection"); }

/** Build bounded metadata writes. Business columns must not point to a new
 * projection until every reader of those columns understands this format. */
export function reportProjectionWrites(id: string, actionID: string, template: Json, bindings: ReportProjectionBinding[]): SQLWrite[] {
  if (!validID(id) || !validID(actionID) || !Array.isArray(bindings) || !bindings.length) invalid();
  const copied = copyJson(template);
  validateBindings(copied, bindings);
  return [
    { sql: "INSERT INTO value_projections(id,action_id,encoding,binding_count,template_json) VALUES(?,?,?,?,?)", params: [id, actionID, "report-projection-v1", bindings.length, { json: copied }], expectChanges: 1 },
    ...bindings.map((binding, ordinal): SQLWrite => ({ sql: "INSERT INTO value_projection_bindings(projection_id,ordinal,report_id,path_json,selector) VALUES(?,?,?,?,?)",
      params: [id, ordinal, binding.reportID, { json: copyJson(binding.path) }, binding.selector], expectChanges: 1 })),
  ];
}

/** Reconstruct a native value only through the original authenticated report.
 * No private template or null slot is returned to callers. */
export async function readReportProjection(store: SQLiteStore, id: string, actionID: string): Promise<Json> {
  return readProjection(store, id, actionID, false);
}

async function readProjection(store: SQLiteStore, id: string, actionID: string, evidence: boolean): Promise<Json> {
  const steps = projectionSteps(store, id, actionID, evidence);
  let step = steps.next();
  while (!step.done) step = steps.next(await (evidence ? readActionReportForEvidence(store, step.value) : readActionReport(store, step.value)));
  return step.value;
}

/** Kept private to synchronous state transitions; public queries yield while
 * reading chunks. Both paths share source, slot and authority validation. */
export function readProjectedJsonSync(store: SQLiteStore, inline: unknown, projectionID: unknown, actionID: string, evidence = false): Json {
  if (projectionID === undefined) invalid();
  if (projectionID === null) return readCommandProjectionSync(store, inline, actionID);
  if (typeof projectionID !== "string" || inline !== "null") invalid();
  const steps = projectionSteps(store, projectionID, actionID, evidence);
  let step = steps.next();
  while (!step.done) step = steps.next(readActionReportSync(store, step.value, evidence));
  return step.value;
}

function* projectionSteps(store: SQLiteStore, id: string, actionID: string, evidence: boolean): Generator<Record<string, unknown>, Json, ActionReport> {
  const assert = () => evidence ? store.assertEvidenceReadAuthority() : store.assertAuthority();
  assert(); const epoch = store.coordinatorEpoch;
  if (!validID(id) || !validID(actionID)) invalid();
  const { template, bindings, frame } = projectionMetadata(store, id, actionID);
  if (frame) {
    const result = readFrameResult(store, frame.source, actionID);
    let output = template;
    for (const path of frame.paths) output = replaceSlot(output, path, copyJson(result));
    assert(); if (store.coordinatorEpoch !== epoch) invalid();
    return output;
  }
  const reports = new Map<string, { reported: Json; phase: "execute" | "reconcile"; parsed?: { result: Json; evidence: Json; measurements: Json } }>();
  let output = template;
  for (const binding of bindings) {
    let source = reports.get(binding.reportID);
    if (!source) {
      const row = store.query("SELECT * FROM action_reports WHERE id=? AND action_id=?", [binding.reportID, actionID])[0];
      if (!row || row.phase !== "execute" && row.phase !== "reconcile") invalid();
      if (row.report_value_id === null) verifyInlineReportReceipt(store, row);
      const envelope = yield row;
      if (envelope.kind !== "report") invalid();
      source = { reported: envelope.value, phase: row.phase };
      reports.set(binding.reportID, source);
    }
    let selected: Json;
    if (binding.selector === "report") selected = source.reported;
    else {
      // The raw report selector is also useful for invalid reports; only
      // derived fields require the payload to pass its phase schema.
      if (!source.parsed) {
        const report = source.phase === "execute" ? validateReport(source.reported, "execute") : validateReport(source.reported, "reconcile");
        const result = source.phase === "execute" ? report.payload : report.payload.kind === "settled" ? report.payload.result : null;
        source.parsed = { result: copyJson(result), evidence: report.evidence, measurements: copyJson(report.measurements) };
      }
      if (binding.selector === "result") selected = source.parsed.result;
      else if (binding.selector === "evidence") selected = source.parsed.evidence;
      else if (binding.selector === "measurements") selected = source.parsed.measurements;
      else {
        const result = source.parsed.result;
        if (!result || typeof result !== "object" || Array.isArray(result) || result.kind !== "succeeded" || !Object.hasOwn(result, "value")) invalid();
        selected = result.value!;
      }
    }
    output = replaceSlot(output, binding.path, copyJson(selected));
  }
  assert(); if (store.coordinatorEpoch !== epoch) invalid();
  return output;
}

/** Legacy inline and explicitly referenced values remain distinguishable. */
export async function readProjectedJson(store: SQLiteStore, inline: unknown, projectionID: unknown, actionID: string): Promise<Json> {
  if (projectionID === undefined) invalid();
  if (projectionID === null) return readCommandProjection(store, inline, false, actionID);
  if (typeof projectionID !== "string" || inline !== "null") invalid();
  return readReportProjection(store, projectionID, actionID);
}

/** Fact readers never see a referenced row's SQL/JSON null slot. */
export async function readFactOutcome(store: SQLiteStore, row: Record<string, unknown>): Promise<Outcome> {
  return readOutcome(store, row, false);
}

/** Fact reconstruction for a fenced recovery proof; never an RPC read gate. */
export async function readFactOutcomeForEvidence(store: SQLiteStore, row: Record<string, unknown>): Promise<Outcome> {
  return readOutcome(store, row, true);
}

async function readOutcome(store: SQLiteStore, row: Record<string, unknown>, evidence: boolean): Promise<Outcome> {
  if (typeof row.action_id !== "string") invalid();
  let outcome: Json;
  if (evidence) {
    store.assertEvidenceReadAuthority(); const epoch = store.coordinatorEpoch;
    if (row.outcome_projection_id === undefined) invalid();
    if (row.outcome_projection_id === null) outcome = await readCommandProjection(store, row.outcome_json, true, row.action_id);
    else {
      if (typeof row.outcome_projection_id !== "string" || row.outcome_json !== "null") invalid();
      outcome = await readProjection(store, row.outcome_projection_id as string, row.action_id, true);
    }
    store.assertEvidenceReadAuthority(); if (store.coordinatorEpoch !== epoch) invalid();
  } else outcome = await readProjectedJson(store, row.outcome_json, row.outcome_projection_id, row.action_id);
  if (!outcome || typeof outcome !== "object" || Array.isArray(outcome)
      || !["succeeded", "failed", "cancelled"].includes(outcome.kind as string)) invalid();
  if (row.outcome_projection_id != null && !["succeeded", "failed"].includes(outcome.kind as string)
      && !store.query("SELECT projection_id FROM frame_result_projections WHERE projection_id=?", [row.outcome_projection_id as string])[0]) invalid();
  return outcome as unknown as Outcome;
}

function projectionMetadata(store: SQLiteStore, id: string, actionID: string) {
  const projection = store.query("SELECT * FROM value_projections WHERE id=? AND action_id=?", [id, actionID])[0];
  if (!projection || projection.encoding !== "report-projection-v1") invalid();
  const template = parseJson(projection.template_json as string);
  const rows = store.query("SELECT * FROM value_projection_bindings WHERE projection_id=? ORDER BY ordinal", [id]);
  const source = store.query("SELECT * FROM frame_result_projections WHERE projection_id=?", [id])[0];
  if (source) {
    const paths = parseJson(source.paths_json as string) as (string | number)[][];
    if (rows.length || !Array.isArray(paths) || !paths.length || paths.length !== projection.binding_count) invalid();
    validateBindings(template, paths.map(path => ({ reportID: source.run_id as string, path, selector: "result" })));
    return { template, bindings: [] as ReportProjectionBinding[], templateBytes: Buffer.byteLength(projection.template_json as string, "utf8"), frame: { source, paths } };
  }
  const bindings = rows.map((row, ordinal): ReportProjectionBinding => {
    if (row.ordinal !== ordinal || typeof row.report_id !== "string" || typeof row.selector !== "string") invalid();
    const path = parseJson(row.path_json as string);
    if (!Array.isArray(path)) invalid();
    return { reportID: row.report_id, path: path as (string | number)[], selector: row.selector as ReportProjectionSelector };
  });
  if (!Number.isSafeInteger(projection.binding_count) || projection.binding_count !== bindings.length || !bindings.length) invalid();
  validateBindings(template, bindings);
  return { template, bindings, templateBytes: Buffer.byteLength(projection.template_json as string, "utf8"), frame: null };
}

/** Startup inventory only: return a conservative byte bound, never a business
 * value or execution permission. This read-only preflight runs before claiming
 * coordinator authority; normal projection reads still require authority. */
export async function reportProjectionByteBound(store: SQLiteStore, id: string, actionID: string, verified = new Set<string>()): Promise<number> {
  const { bindings, templateBytes, frame } = projectionMetadata(store, id, actionID);
  let bytes = templateBytes;
  if (frame) {
    for (const part of canonicalJsonChunks(readFrameResult(store, frame.source, actionID))) {
      bytes += part.byteLength * frame.paths.length;
      if (!Number.isSafeInteger(bytes)) invalid();
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    return bytes;
  }
  for (const binding of bindings) {
    const row = store.query("SELECT * FROM action_reports WHERE id=? AND action_id=?", [binding.reportID, actionID])[0];
    if (!row) invalid();
    const ref = actionReportValueReference(store, row);
    if (!ref) {
      verifyInlineReportReceipt(store, row);
      bytes += Buffer.byteLength(row.report_json as string, "utf8");
    } else {
      if (!verified.has(ref.id)) {
        const hash = createHash("sha256"), utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
        let length = 0;
        while (length < ref.length) {
          const chunk = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [ref.id, length])[0];
          if (!chunk || !(chunk.payload_blob instanceof Uint8Array) || chunk.length_bytes !== chunk.payload_blob.byteLength
              || !chunk.payload_blob.byteLength || chunk.payload_blob.byteLength > 65_536 || chunk.payload_blob.byteLength > ref.length - length
              || createHash("sha256").update(chunk.payload_blob).digest("hex") !== chunk.chunk_digest) invalid();
          try { utf8.decode(chunk.payload_blob, { stream: true }); } catch { invalid(); }
          hash.update(chunk.payload_blob); length += chunk.payload_blob.byteLength;
          await new Promise<void>(resolve => setImmediate(resolve));
        }
        try { utf8.decode(); } catch { invalid(); }
        const current = actionReportValueReference(store, row);
        if (hash.digest("hex") !== ref.digest || !current || current.id !== ref.id || current.owner !== ref.owner
            || current.encoding !== ref.encoding || current.length !== ref.length || current.digest !== ref.digest) invalid();
        verified.add(ref.id);
      }
      bytes += ref.length;
    }
    if (!Number.isSafeInteger(bytes)) invalid();
  }
  return bytes;
}

function validID(value: unknown): value is string { return typeof value === "string" && value.length > 0 && value.trim() === value; }
function validateBindings(template: Json, bindings: ReportProjectionBinding[]): void {
  const paths: (string | number)[][] = [];
  for (const binding of bindings) {
    if (!binding || !validID(binding.reportID) || !selectors.has(binding.selector) || !Array.isArray(binding.path)
        || binding.path.some(part => typeof part === "string" ? !part.length : !Number.isSafeInteger(part) || part < 0)) invalid();
    if (paths.some(path => prefix(path, binding.path) || prefix(binding.path, path))) invalid();
    if (slot(template, binding.path) !== null) invalid();
    paths.push(binding.path);
  }
}
function prefix(left: (string | number)[], right: (string | number)[]): boolean {
  return left.length <= right.length && left.every((part, index) => part === right[index]);
}
function slot(root: Json, path: (string | number)[]): Json {
  let value: Json = root;
  for (const part of path) {
    if (!value || typeof value !== "object" || Array.isArray(value) !== (typeof part === "number") || !Object.hasOwn(value, part)) invalid();
    value = (value as { [key: string]: Json })[part]!;
  }
  return value;
}
function replaceSlot(root: Json, path: (string | number)[], replacement: Json): Json {
  if (!path.length) return replacement;
  const parent = slot(root, path.slice(0, -1));
  if (!parent || typeof parent !== "object") invalid();
  Object.defineProperty(parent, path.at(-1)!, { value: replacement, enumerable: true, writable: true, configurable: true });
  return root;
}
