import { createHash, randomUUID } from "node:crypto";
import { canonicalJsonChunks, harnessError, type ErrorInfo } from "../contract/index.js";
import type { ReportFragmentOwner } from "./canonical-report-staging.js";

interface Entry {
  owner: ReportFragmentOwner; transferID: string; chunks: Uint8Array[]; total: number; digest: string; frameBytes: number; expiresAt: number;
}
export interface BoundErrorFragment { id: string; offset: number; total: number; digest: string; bytes: Uint8Array }

/** Ephemeral bounded snapshots only. Restart invalidates tokens and callers
 * must retry the original report identity; no business result lives here. */
export class BoundErrorFragments {
  readonly #entries = new Map<string, Entry>();
  readonly #timer = setInterval(() => this.expire(), 30_000).unref();
  #bytes = 0;
  #building = 0;
  #closed = false;
  constructor(readonly quotaBytes = 67_108_864) {
    if (!Number.isSafeInteger(quotaBytes) || quotaBytes <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid error snapshot quota");
  }

  async create(owner: ReportFragmentOwner, transferID: string, error: ErrorInfo, frameBytes: number): Promise<BoundErrorFragment> {
    this.expire();
    if (this.#closed || !Number.isSafeInteger(frameBytes) || frameBytes <= 0 || frameBytes > 65_536 || this.#entries.size + this.#building >= 128)
      throw harnessError("LIMIT_EXCEEDED", "Error snapshot capacity is unavailable");
    const workerHash = createHash("sha256"); for (const part of canonicalJsonChunks([owner.domainID, owner.principalID, owner.nodeID])) workerHash.update(part);
    const source = canonicalJsonChunks({ version: 4, transferID, workerDigest: workerHash.digest("hex"), ok: false, error });
    const chunks: Uint8Array[] = [], hash = createHash("sha256"); let total = 0, count = 0;
    this.#building++;
    try {
      for (const chunk of source) {
        if (chunk.byteLength > this.quotaBytes - this.#bytes) throw harnessError("LIMIT_EXCEEDED", "Error snapshot capacity is unavailable");
        const data = Uint8Array.from(chunk); chunks.push(data); this.#bytes += data.length; total += data.length; hash.update(data);
        if (++count % 16 === 0) await new Promise<void>(resolve => setImmediate(resolve));
        if (this.#closed) throw harnessError("NODE_STATE_INVALID", "Error snapshot cache is closed");
      }
      const id = randomUUID(), entry: Entry = { owner, transferID, chunks, total, digest: hash.digest("hex"), frameBytes, expiresAt: Date.now() + 120_000 };
      this.#entries.set(id, entry);
      return this.slice(id, entry, 0);
    } catch (error) { this.#bytes -= total; throw error; }
    finally { this.#building--; }
  }

  get(id: string): Entry | undefined { this.expire(); return this.#entries.get(id); }
  read(id: string, entry: Entry, offset: number): BoundErrorFragment {
    this.expire();
    if (this.#closed || this.#entries.get(id) !== entry) throw harnessError("FORBIDDEN", "Error fragment is unavailable");
    if (!Number.isSafeInteger(offset) || offset < 0 || offset >= entry.total || offset % entry.frameBytes)
      throw harnessError("INVALID_ARGUMENT", "Invalid error fragment offset");
    entry.expiresAt = Date.now() + 120_000;
    return this.slice(id, entry, offset);
  }
  release(id: string): void { const entry = this.#entries.get(id); if (entry) { this.#entries.delete(id); this.#bytes -= entry.total; } }
  close(): void { this.#closed = true; clearInterval(this.#timer); for (const id of this.#entries.keys()) this.release(id); }

  private slice(id: string, entry: Entry, offset: number): BoundErrorFragment {
    const count = Math.min(entry.frameBytes, entry.total - offset), bytes = new Uint8Array(count);
    let written = 0, position = offset;
    while (written < count) {
      const index = Math.floor(position / 65_536), within = position % 65_536, chunk = entry.chunks[index];
      if (!chunk || within >= chunk.length) throw harnessError("INTERNAL_ERROR", "Error snapshot is incomplete");
      const take = Math.min(count - written, chunk.length - within);
      bytes.set(chunk.subarray(within, within + take), written); written += take; position += take;
    }
    return { id, offset, total: entry.total, digest: entry.digest, bytes };
  }
  private expire(): void { const now = Date.now(); for (const [id, entry] of this.#entries) if (entry.expiresAt <= now) this.release(id); }
}
