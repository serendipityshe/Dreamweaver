import { Worker } from "node:worker_threads";
import { copyJson, harnessError } from "../contract/index.js";
import type { SQLiteStore, ValuePublication } from "./sqlite.js";
import { ValueStore } from "./value-store.js";

/** An internal token issued only after a complete, byte-exact worker check. */
export interface VerifiedReport { readonly __verifiedReport: unique symbol }
export interface ReportLengths { valueBytes: number; payloadBytes: number | null; evidenceBytes: number | null }
type Evidence = { store: SQLiteStore; epoch: number; ref: ValuePublication; lengths: ReportLengths };
const proofs = new WeakMap<object, Evidence>();

/** Validate a sealed report without returning the parsed business object to
 * the coordinator thread. The second read compares actual stored bytes with
 * the canonical re-encoding, not merely a caller-supplied digest. */
export async function verifyReportValue(store: SQLiteStore, ref: ValuePublication): Promise<VerifiedReport> {
  ref = copyJson(ref) as unknown as ValuePublication;
  store.assertAuthority(); const epoch = store.coordinatorEpoch, values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  const status = values.status(ref.id, ref.owner);
  if (status.state !== "sealed" && status.state !== "published" || status.encoding !== ref.encoding || status.length !== ref.length
      || status.digest !== ref.digest || status.written !== ref.length) throw harnessError("STATE_CONFLICT", "Report value is unavailable");
  const filename = import.meta.url.endsWith(".ts") ? "./report-validation-worker.ts" : "./report-validation-worker.js";
  const worker = new Worker(new URL(filename, import.meta.url), import.meta.url.endsWith(".ts") ? { execArgv: ["--import", "tsx"] } : {});
  let sequence = 0, failure: Error | undefined;
  const pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();
  const failed = () => { failure = new Error("Report validation worker is unavailable"); for (const entry of pending.values()) entry.reject(failure); pending.clear(); };
  worker.on("error", failed); worker.on("exit", failed);
  worker.on("message", (message: { id: number; ok: boolean; result?: unknown }) => {
    const entry = pending.get(message.id); if (!entry) return;
    pending.delete(message.id);
    if (message.ok) entry.resolve(message.result); else entry.reject(harnessError("INVALID_ARGUMENT", "Report bytes failed validation"));
  });
  const call = (method: string, bytes?: Uint8Array): Promise<unknown> => {
    if (failure) return Promise.reject(failure);
    const id = ++sequence, data = bytes ? Uint8Array.from(bytes) : undefined;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      try { worker.postMessage({ id, method, ...(data ? { bytes: data } : {}) }, data ? [data.buffer] : []); }
      catch (error) { pending.delete(id); reject(error); }
    });
  };
  try {
    for await (const chunk of values.read(ref.id, ref.owner)) await call("read", chunk);
    const result = await call("finish") as { length?: unknown; digest?: unknown; valueBytes?: unknown; payloadBytes?: unknown; evidenceBytes?: unknown };
    const validSize = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value > 0;
    if (!result || result.length !== ref.length || result.digest !== ref.digest || !validSize(result.valueBytes)
        || result.payloadBytes !== null && !validSize(result.payloadBytes) || result.evidenceBytes !== null && !validSize(result.evidenceBytes))
      throw harnessError("INVALID_ARGUMENT", "Report validation result does not match its manifest");
    for await (const chunk of values.read(ref.id, ref.owner)) await call("compare", chunk);
    await call("verified");
    store.assertAuthority();
    const current = values.status(ref.id, ref.owner);
    if (store.coordinatorEpoch !== epoch || current.state !== "sealed" && current.state !== "published"
        || current.encoding !== ref.encoding || current.length !== ref.length || current.digest !== ref.digest || current.written !== ref.length)
      throw harnessError("EXECUTION_FENCED", "Report verification authority changed");
    const proof = Object.freeze({}) as VerifiedReport;
    proofs.set(proof, { store, epoch, ref: { ...ref }, lengths: { valueBytes: result.valueBytes,
      payloadBytes: result.payloadBytes as number | null, evidenceBytes: result.evidenceBytes as number | null } });
    return proof;
  } finally { await worker.terminate(); }
}

/** Recheck the token against the same store, epoch and immutable manifest. */
export function inspectVerifiedReport(store: SQLiteStore, ref: ValuePublication, proof: VerifiedReport): ReportLengths {
  const evidence = proof && typeof proof === "object" ? proofs.get(proof) : undefined;
  store.assertAuthority();
  if (!evidence || evidence.store !== store || evidence.epoch !== store.coordinatorEpoch || evidence.ref.id !== ref.id
      || evidence.ref.owner !== ref.owner || evidence.ref.encoding !== ref.encoding || evidence.ref.length !== ref.length || evidence.ref.digest !== ref.digest)
    throw harnessError("FORBIDDEN", "Report verification proof is invalid");
  const current = new ValueStore(store, Number.MAX_SAFE_INTEGER).status(ref.id, ref.owner);
  if (current.state !== "sealed" && current.state !== "published" || current.encoding !== ref.encoding || current.length !== ref.length
      || current.digest !== ref.digest || current.written !== ref.length) throw harnessError("STATE_CONFLICT", "Report value is unavailable");
  return { ...evidence.lengths };
}
