import { createHash, timingSafeEqual } from "node:crypto";
import { canonicalJsonChunks, harnessError, InternalCanonicalJsonDecoder, type Json } from "../contract/index.js";
import { ValueStore, type ValuePublication } from "../persistence/index.js";
import { ReportTransferConcurrency } from "./report-transfer-concurrency.js";

export interface ReportFragmentOwner { domainID: string; principalID: string; nodeID: string }
export interface ReportFragment { transferID: string; offset: number; total: number; digest: string }
export type StagedReportResult = { complete: false; nextOffset: number }
  | { complete: true; value: Json; publication: ValuePublication };

export function reportStagingOwner(owner: ReportFragmentOwner): string {
  const hash = createHash("sha256"); for (const part of canonicalJsonChunks([owner.domainID, owner.principalID, owner.nodeID])) hash.update(part);
  return `report-owner:${hash.digest("hex")}`;
}

/** Check complete stored bytes and canonical structure before any use. */
export async function readCanonicalStagedValue(values: ValueStore, publication: ValuePublication): Promise<Json> {
  const parser = new InternalCanonicalJsonDecoder(), hash = createHash("sha256"); let read = 0;
  for await (const chunk of values.read(publication.id, publication.owner)) { read += chunk.byteLength; hash.update(chunk); parser.write(chunk); }
  if (read !== publication.length || hash.digest("hex") !== publication.digest)
    throw harnessError("INVALID_ARGUMENT", "Staged report digest does not match");
  const value = parser.finish();
  const canonical = canonicalJsonChunks(value)[Symbol.iterator]();
  for await (const chunk of values.read(publication.id, publication.owner)) {
    const next = canonical.next();
    if (next.done || next.value.byteLength !== chunk.byteLength || !timingSafeEqual(next.value, chunk))
      throw harnessError("INVALID_ARGUMENT", "Report bytes are not canonical JSON");
  }
  if (!canonical.next().done) throw harnessError("INVALID_ARGUMENT", "Report bytes are not canonical JSON");
  return value;
}

/** Durable staging for canonical report bodies only, never RPC envelopes.
 * The caller must derive owner from fresh authentication, not fragment data.
 * Staging and sealing do not authorize a report or publish a business result. */
export class CanonicalReportStaging {
  readonly #flights = new Map<string, Promise<void>>();
  #waiting = 0;
  constructor(readonly values: ValueStore, readonly maxTransferBytes: number, readonly concurrency = new ReportTransferConcurrency()) {
    if (!Number.isSafeInteger(maxTransferBytes) || maxTransferBytes <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid report staging limit");
  }

  async accept(owner: ReportFragmentOwner, meta: ReportFragment, bytes: Uint8Array): Promise<StagedReportResult> {
    return this.concurrency.run(() => this.acceptAdmitted(owner, meta, bytes));
  }

  private async acceptAdmitted(owner: ReportFragmentOwner, meta: ReportFragment, bytes: Uint8Array): Promise<StagedReportResult> {
    if (!owner || typeof owner.domainID !== "string" || !owner.domainID || typeof owner.principalID !== "string" || !owner.principalID
        || typeof owner.nodeID !== "string" || !owner.nodeID
        || !meta || !/^[A-Za-z0-9_-]{1,128}$/.test(meta.transferID) || !Number.isSafeInteger(meta.offset) || meta.offset < 0
        || !Number.isSafeInteger(meta.total) || meta.total <= 0 || !/^[a-f0-9]{64}$/.test(meta.digest)
        || !(bytes instanceof Uint8Array) || !bytes.byteLength
        || meta.offset >= meta.total || meta.offset % 65_536 || bytes.length !== Math.min(65_536, meta.total - meta.offset))
      throw harnessError("INVALID_ARGUMENT", "Invalid canonical report fragment");
    if (meta.total > this.maxTransferBytes) throw harnessError("LIMIT_EXCEEDED", "Canonical report staging is full");
    const ownerID = reportStagingOwner(owner);
    const id = `report-transfer:${createHash("sha256").update(ownerID).update("\0").update(meta.transferID).digest("hex")}`;
    if (this.#waiting >= 64 || !this.#flights.has(id) && this.#flights.size >= 64)
      throw harnessError("LIMIT_EXCEEDED", "Canonical report staging is busy");
    this.#waiting++;
    try { while (this.#flights.has(id)) await this.#flights.get(id); } finally { this.#waiting--; }
    let release!: () => void;
    this.#flights.set(id, new Promise<void>(resolve => { release = resolve; }));
    try {
      if (meta.offset !== 0) this.values.status(id, ownerID);
      this.values.begin({ id, owner: ownerID, encoding: "utf8-v1", length: meta.total, digest: meta.digest });
      const state = this.values.append(id, ownerID, meta.offset, bytes);
      // A replay of an early frame must not trigger finalization merely
      // because a later attempt already staged the remaining bytes.
      if (meta.offset + bytes.length < meta.total) return { complete: false, nextOffset: meta.offset + bytes.length };
      if (state.written !== state.length) throw harnessError("STATE_CONFLICT", "Canonical report is incomplete");
      const publication = await this.values.seal(id, ownerID);
      const value = await readCanonicalStagedValue(this.values, publication);
      return { complete: true, value, publication };
    } finally {
      this.#flights.delete(id); release();
    }
  }
}
