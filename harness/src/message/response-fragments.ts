import { createHash, randomUUID } from "node:crypto";
import { harnessError, type RPCRequest, type NodeRPCRequest, type SessionBinding } from "../contract/index.js";

/** One coordinator-local immutable snapshot. A lost coordinator invalidates
 * its tokens; clients must not substitute a new result for an old snapshot. */
interface Entry {
  binding: SessionBinding;
  request: RPCRequest | NodeRPCRequest;
  node: boolean;
  error: boolean;
  result: unknown;
  authorization?: unknown;
  chunks: Buffer[];
  total: number;
  digest: string;
  chunkBytes: number;
  expiresAt: number;
}
export interface ResponseFragment { id: string; offset: number; total: number; digest: string; bytes: Buffer }

export class ResponseFragments {
  readonly #entries = new Map<string, Entry>();
  #bytes = 0;
  #pending = 0;
  #closed = false;
  // Logical bytes retained by this transient cache, not a JavaScript string
  // capacity. Individual snapshots are stored in bounded chunks.
  readonly #quota = 512 * 1024 * 1024;
  readonly #timer = setInterval(() => this.expire(), 30_000).unref();

  create(binding: SessionBinding, request: RPCRequest | NodeRPCRequest, result: unknown, body: string, chunkBytes: number, node = false, error = false, authorization?: unknown): ResponseFragment {
    this.expire();
    const size = Buffer.byteLength(body, "utf8");
    if (this.#closed || !Number.isSafeInteger(chunkBytes) || chunkBytes <= 0 || size > this.#quota - this.#bytes || this.#entries.size + this.#pending >= 128) {
      throw harnessError("LIMIT_EXCEEDED", "Fragment snapshot capacity is unavailable");
    }
    const bytes = Buffer.from(body, "utf8"), id = randomUUID();
    const chunks: Buffer[] = [];
    for (let offset = 0; offset < bytes.length; offset += chunkBytes) chunks.push(bytes.subarray(offset, Math.min(offset + chunkBytes, bytes.length)));
    const entry: Entry = { binding, request, result, node, error, authorization, chunks, total: bytes.length, digest: createHash("sha256").update(bytes).digest("hex"),
      chunkBytes, expiresAt: Date.now() + 120_000 };
    this.#entries.set(id, entry); this.#bytes += bytes.length;
    return this.slice(id, entry, 0);
  }

  /** Build an immutable response snapshot from bounded canonical bytes. The
   * reservation is taken before yielding so concurrent builders share a quota. */
  async createChunks(binding: SessionBinding, request: RPCRequest | NodeRPCRequest, result: unknown, parts: Iterable<Uint8Array>,
    total: number, chunkBytes: number, node = false, authorization?: unknown): Promise<ResponseFragment> {
    this.expire();
    if (this.#closed || !Number.isSafeInteger(total) || total <= 0 || !Number.isSafeInteger(chunkBytes) || chunkBytes <= 0
        || total > this.#quota - this.#bytes || this.#entries.size + this.#pending >= 128)
      throw harnessError("LIMIT_EXCEEDED", "Fragment snapshot capacity is unavailable");
    this.#bytes += total; this.#pending++;
    let published = false;
    try {
      const chunks: Buffer[] = [], hash = createHash("sha256"); let written = 0, used = 0, current = Buffer.allocUnsafe(Math.min(chunkBytes, total)), count = 0;
      for (const part of parts) {
        if (!(part instanceof Uint8Array) || !part.length || part.length > 65_536 || part.length > total - written)
          throw harnessError("INTERNAL_ERROR", "Invalid response snapshot chunk");
        hash.update(part); let offset = 0;
        while (offset < part.length) {
          const length = Math.min(current.length - used, part.length - offset);
          current.set(part.subarray(offset, offset + length), used); used += length; offset += length; written += length;
          if (used === current.length) {
            chunks.push(current); used = 0;
            if (written < total) current = Buffer.allocUnsafe(Math.min(chunkBytes, total - written));
          }
        }
        if (++count % 8 === 0) await new Promise<void>(resolve => setImmediate(resolve));
      }
      if (written !== total || used !== 0 || this.#closed) throw harnessError("INTERNAL_ERROR", "Incomplete response snapshot");
      const id = randomUUID(), entry: Entry = { binding, request, result, node, error: false, authorization, chunks, total,
        digest: hash.digest("hex"), chunkBytes, expiresAt: Date.now() + 120_000 };
      this.#entries.set(id, entry); published = true;
      return this.slice(id, entry, 0);
    } finally { this.#pending--; if (!published) this.#bytes -= total; }
  }

  get(id: string): Entry | undefined {
    this.expire();
    return this.#entries.get(id);
  }

  read(id: string, entry: Entry, offset: number): ResponseFragment {
    if (!Number.isSafeInteger(offset) || offset < 0 || offset >= entry.total || offset % entry.chunkBytes !== 0) {
      throw harnessError("INVALID_ARGUMENT", "Invalid response fragment offset");
    }
    entry.expiresAt = Date.now() + 120_000;
    return this.slice(id, entry, offset);
  }

  release(id: string): void {
    const entry = this.#entries.get(id);
    if (entry) { this.#entries.delete(id); this.#bytes -= entry.total; }
  }
  close(): void { this.#closed = true; clearInterval(this.#timer); for (const id of this.#entries.keys()) this.release(id); }

  private slice(id: string, entry: Entry, offset: number): ResponseFragment {
    return { id, offset, total: entry.total, digest: entry.digest, bytes: entry.chunks[offset / entry.chunkBytes]! };
  }
  private expire(): void {
    const now = Date.now();
    for (const [id, entry] of this.#entries) if (entry.expiresAt <= now) { this.#entries.delete(id); this.#bytes -= entry.total; }
  }
}
