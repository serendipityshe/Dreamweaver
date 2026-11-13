import { createHash, randomUUID } from "node:crypto";
import { InternalCanonicalJsonDecoder, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, parseJson, type AppliedPolicy, type Json } from "../contract/index.js";
import { ValueStore } from "./value-store.js";
import type { SQLiteStore, SQLWrite, ValuePublication } from "./sqlite.js";

type Obj = Record<string, Json>;
const object = (v: unknown): v is Obj => !!v && typeof v === "object" && !Array.isArray(v);
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
function invalid(): never { throw harnessError("INTERNAL_ERROR", "Invalid fixed policy source"); }
export function samePolicyValue(a: unknown, b: unknown): boolean {
  const x = canonicalJsonChunks(a)[Symbol.iterator](), y = canonicalJsonChunks(b)[Symbol.iterator]();
  for (;;) { const p = x.next(), q = y.next(); if (p.done || q.done) return p.done === q.done; if (!Buffer.from(p.value).equals(q.value)) return false; }
}
export interface PolicySource { encoding: "node-policies-v1"; principalID: string; nodeID: string; incarnation: string; value: ValuePublication }
export function policyMetadata(policies: AppliedPolicy[]): Json { return policies.map(p => ({ ref: { ...p.ref }, config: null })); }
export function storedPolicySource(value: unknown): PolicySource | null {
  const body = typeof value === "string" ? parseJson(value) : value;
  if (!object(body) || body.policySource === undefined) return null;
  const source = body.policySource as unknown as PolicySource;
  if (!source || source.encoding !== "node-policies-v1" || typeof source.principalID !== "string" || typeof source.nodeID !== "string" || typeof source.incarnation !== "string"
      || !source.value || source.value.encoding !== "utf8-v1" || !Number.isSafeInteger(source.value.length) || source.value.length <= 0 || !/^[a-f0-9]{64}$/.test(source.value.digest)) invalid();
  return source;
}
export async function stageNodePolicies(store: SQLiteStore, principalID: string, params: { nodeID: string; incarnation: string; policies: AppliedPolicy[] }, quota: number, assertCaller: () => void): Promise<PolicySource | null> {
  const epoch = store.coordinatorEpoch, check = () => { store.assertAuthority(); if (epoch !== store.coordinatorEpoch) throw harnessError("EXECUTION_FENCED", "Policy staging owner changed"); assertCaller(); };
  let length = 0; const hash = createHash("sha256");
  for (const part of canonicalJsonChunks(params.policies)) { check(); length += part.length; hash.update(part); await new Promise<void>(r => setImmediate(r)); }
  if (!Number.isSafeInteger(length)) throw harnessError("LIMIT_EXCEEDED", "Policy configuration exceeds supported length");
  if (length <= 16384) return null;
  const value: ValuePublication = { id: `node-policies:${randomUUID()}`, owner: `node-policies:${store.domainID}`, encoding: "utf8-v1", length, digest: hash.digest("hex") };
  const source: PolicySource = { encoding: "node-policies-v1", principalID, nodeID: params.nodeID, incarnation: params.incarnation, value }, values = new ValueStore(store, quota);
  const guards = () => [write("UPDATE node_policy_staging SET state='active' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [value.id, epoch])];
  try {
    check(); values.begin(value, [write("INSERT INTO node_policy_staging(value_id,coordinator_epoch,state) VALUES(?,?,?)", [value.id, epoch, "active"])]);
    let offset = 0;
    for (const part of canonicalJsonChunks(params.policies)) { check(); values.append(value.id, value.owner, offset, part, guards()); offset += part.length; await new Promise<void>(r => setImmediate(r)); }
    check(); await values.seal(value.id, value.owner); check(); store.checkGuards(guards()); return source;
  } catch (error) { abandonNodePolicies(store, source); throw error; }
}
export function abandonNodePolicies(store: SQLiteStore, source: PolicySource | null): void {
  if (!source || store.requiresRecovery) return;
  try { store.assertAuthority(); const row = store.query("SELECT state,coordinator_epoch FROM node_policy_staging WHERE value_id=?", [source.value.id])[0];
    if (!row || row.state !== "active" || row.coordinator_epoch !== store.coordinatorEpoch) return;
    store.commit({ operationID: `node-policies.abandon:${source.value.id}`, kind: "node_policy.abandoned", subjectID: source.value.id, request: { valueID: source.value.id },
      writes: [write("UPDATE node_policy_staging SET state='abandoned' WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, row.coordinator_epoch as number])], evidence: null, result: null });
  } catch { /* Fenced or unknown storage is owned by the recovery path. */ }
}
export function policyPublicationWrites(source: PolicySource | null, epoch: number): SQLWrite[] { return source ? [write("DELETE FROM node_policy_staging WHERE value_id=? AND coordinator_epoch=? AND state='active'", [source.value.id, epoch])] : []; }
export async function retireNodePolicies(store: SQLiteStore): Promise<void> {
  const values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  if (store.query("SELECT s.value_id FROM node_policy_staging s JOIN value_manifests m ON m.id=s.value_id WHERE m.state='deleting' LIMIT 1")[0]) await values.resumeUnboundRetirements();
  const rows = store.query(`SELECT m.*,s.state staging_state,s.coordinator_epoch FROM node_policy_staging s JOIN value_manifests m ON m.id=s.value_id
    WHERE m.state IN ('staging','sealed') AND (s.state='abandoned' OR s.coordinator_epoch<?) AND NOT EXISTS(SELECT 1 FROM value_references v WHERE v.value_id=m.id) LIMIT 16`, [store.coordinatorEpoch]);
  for (const row of rows) await values.retireUnbound({ id: row.id as string, owner: row.owner as string, encoding: row.encoding as string, length: row.total_bytes as number, digest: row.digest as string,
    written: row.written_bytes as number, revision: row.revision as number, state: row.state as "staging" | "sealed" }, [write("UPDATE node_policy_staging SET state=? WHERE value_id=? AND coordinator_epoch=? AND state=?", [row.staging_state as string, row.id as string, row.coordinator_epoch as number, row.staging_state as string])]);
}
export function encodeNodeConfiguration(params: unknown, source: PolicySource | null): Json {
  const body = copyJson(params); if (!object(body)) invalid();
  return source ? copyJson({ ...body, policies: policyMetadata(body.policies as unknown as AppliedPolicy[]), policySource: source }) : body;
}
export function verifyPolicySource(store: SQLiteStore, source: PolicySource, metadata?: unknown): void {
  const row = store.query(`SELECT s.configuration_json,m.owner,m.encoding,m.total_bytes,m.written_bytes,m.digest,m.state,h.request_json
    FROM node_sessions s JOIN value_manifests m ON m.id=? JOIN value_references v ON v.value_id=m.id
    JOIN history h ON h.operation_id=v.operation_id AND h.type='node.registered' AND h.subject_id=s.node_id
    WHERE s.incarnation=? AND s.node_id=? AND s.principal_id=?`, [source.value.id, source.incarnation, source.nodeID, source.principalID])[0];
  const ref = source.value;
  if (!row || row.owner !== ref.owner || row.encoding !== ref.encoding || row.total_bytes !== ref.length || row.written_bytes !== ref.length || row.digest !== ref.digest || row.state !== "published") invalid();
  const config = parseJson(row.configuration_json as string), request = parseJson(row.request_json as string);
  if (!object(config) || !object(request) || request.principalID !== source.principalID || canonicalJsonDigest(config.policySource) !== canonicalJsonDigest(source)
      || canonicalJsonDigest(request) !== canonicalJsonDigest({ principalID: source.principalID, ...config })) invalid();
  if (metadata !== undefined && !samePolicyValue(metadata, config.policies)) invalid();
}
export function* policySourceSteps(store: SQLiteStore, source: PolicySource): Generator<void, AppliedPolicy[]> {
  verifyPolicySource(store, source); const hash = createHash("sha256"), decoder = new InternalCanonicalJsonDecoder(); let offset = 0;
  while (offset < source.value.length) {
    const row = store.query("SELECT length_bytes,chunk_digest,payload_blob FROM value_chunks WHERE value_id=? AND offset_bytes=?", [source.value.id, offset])[0];
    if (!row || !(row.payload_blob instanceof Uint8Array) || !row.payload_blob.length || row.payload_blob.length !== row.length_bytes || row.payload_blob.length > 65536
        || row.payload_blob.length > source.value.length - offset || createHash("sha256").update(row.payload_blob).digest("hex") !== row.chunk_digest) invalid();
    decoder.write(row.payload_blob); hash.update(row.payload_blob); offset += row.payload_blob.length; yield;
  }
  if (hash.digest("hex") !== source.value.digest) invalid(); const policies = decoder.finish();
  if (!Array.isArray(policies) || policies.some(p => !object(p) || !object(p.ref) || typeof p.ref.id !== "string" || typeof p.ref.version !== "string" || !Object.hasOwn(p, "config"))) invalid();
  verifyPolicySource(store, source); return policies as unknown as AppliedPolicy[];
}
export function readPolicySource(store: SQLiteStore, source: PolicySource): AppliedPolicy[] {
  const steps = policySourceSteps(store, source); for (;;) { const step = steps.next(); if (step.done) return step.value; }
}
export function readNodeConfiguration(store: SQLiteStore, encoded: unknown): Json {
  const body = typeof encoded === "string" ? parseJson(encoded) : copyJson(encoded); if (!object(body)) invalid();
  const source = storedPolicySource(body); if (!source) return body;
  const policies = readPolicySource(store, source);
  if (canonicalJsonDigest(body.policies) !== canonicalJsonDigest(policyMetadata(policies))) invalid();
  const { policySource: _source, ...metadata } = body; return { ...metadata, policies: copyJson(policies) };
}
export function encodePreparationPolicies(policies: AppliedPolicy[], source: PolicySource | null): Json { return source ? copyJson({ policySource: source, policies: policyMetadata(policies) }) : copyJson(policies); }
export function readPreparationPolicies(store: SQLiteStore, prep: Record<string, unknown>): AppliedPolicy[] {
  const value = parseJson(prep.policies_json as string);
  if (Array.isArray(value)) return value as unknown as AppliedPolicy[];
  return (readNodeConfiguration(store, value) as Obj).policies as unknown as AppliedPolicy[];
}
export function policyPayloadSource(store: SQLiteStore, payload: unknown): PolicySource | null {
  if (!object(payload) || !object(payload.context)) return null;
  const context = payload.context, explicit = storedPolicySource(context); if (explicit) return explicit;
  if (typeof context.actionID === "string") {
    const row = store.query("SELECT request_json FROM actions WHERE id=?", [context.actionID])[0];
    return row ? storedPolicySource(row.request_json) : null;
  }
  if (typeof context.frameID === "string") {
    const row = store.query("SELECT policies_json FROM dispatch_preparations WHERE frame_id=?", [context.frameID])[0];
    return row ? storedPolicySource(row.policies_json) : null;
  }
  return null;
}
export function expandPolicyPayload(store: SQLiteStore, payload: Json): Json {
  if (!object(payload) || !object(payload.context)) return payload;
  const source = storedPolicySource(payload.context); if (!source) return payload;
  const policies = readPolicySource(store, source), context = payload.context;
  if (canonicalJsonDigest(context.policies) !== canonicalJsonDigest(policyMetadata(policies))) invalid();
  const { policySource: _source, ...metadata } = context;
  return { ...payload, context: { ...metadata, policies: copyJson(policies), ...(object(context.prepared) ? { prepared: { ...context.prepared, policies: copyJson(policies) } } : {}) } };
}
