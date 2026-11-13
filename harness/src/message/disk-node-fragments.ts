import { constants as bufferConstants } from "node:buffer";
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { open, readFile, unlink } from "node:fs/promises";
import { join } from "node:path";
import { canonicalJsonChunks, InternalCanonicalJsonDecoder, harnessError, HarnessError, type Json } from "../contract/index.js";
import type { NodeFragmentMeta, NodeFragmentOwner, NodeFragmentResult } from "./node-fragments.js";
import { DiskFragmentLease } from "./disk-fragment-lease.js";

interface Entry {
  path: string; total: number; digest: string; offset: number; expires: number;
  hash: ReturnType<typeof createHash>; complete: boolean; failed: boolean;
}
export type CanonicalNodeFragmentResult = { complete: false; nextOffset: number } | { complete: true; value: Json };

/** Coordinator-local, disposable staging; the sender retains the authoritative value.
 * Files are private to this instance and never treated as evidence of a business commit.
 */
export class DiskNodeFragments {
  readonly #entries = new Map<string, Entry>();
  #reserved = 0;
  #closed = false;
  #queued = 0;
  #tail: Promise<void> = Promise.resolve();
  readonly directory: string;
  private constructor(private readonly lease: DiskFragmentLease, readonly quota: number, readonly ttlMs: number) { this.directory = lease.directory; }

  static async create(parent: string, quota: number, ttlMs = 120_000): Promise<DiskNodeFragments> {
    if (!parent || !Number.isSafeInteger(quota) || quota <= 0 || !Number.isSafeInteger(ttlMs) || ttlMs <= 0)
      throw harnessError("INVALID_ARGUMENT", "Invalid disk fragment staging configuration");
    return new DiskNodeFragments(new DiskFragmentLease(parent, quota), quota, ttlMs);
  }

  accept(owner: NodeFragmentOwner, meta: NodeFragmentMeta, bytes: Uint8Array): Promise<NodeFragmentResult>;
  accept(owner: NodeFragmentOwner, meta: NodeFragmentMeta, bytes: Uint8Array, canonical: true, assertCurrent: () => void): Promise<CanonicalNodeFragmentResult>;
  accept(owner: NodeFragmentOwner, meta: NodeFragmentMeta, bytes: Uint8Array, canonical = false, assertCurrent = () => {}): Promise<NodeFragmentResult | CanonicalNodeFragmentResult> {
    if (this.#queued >= 64) return Promise.reject(harnessError("LIMIT_EXCEEDED", "Node fragment staging is busy"));
    this.#queued++;
    return this.#serial<NodeFragmentResult | CanonicalNodeFragmentResult>(async () => {
      assertCurrent();
      if (this.#closed) throw harnessError("NODE_STATE_INVALID", "Node fragment receiver is closed");
      if (!owner || !owner.domainID || !owner.principalID || !meta || !/^[A-Za-z0-9_-]{1,128}$/.test(meta.transferID)
          || !Number.isSafeInteger(meta.offset) || meta.offset < 0 || !Number.isSafeInteger(meta.total) || meta.total <= 0
          || !/^[a-f0-9]{64}$/.test(meta.digest) || !(bytes instanceof Uint8Array) || !bytes.byteLength)
        throw harnessError("INVALID_ARGUMENT", "Invalid node fragment");
      const frame = 1_048_576;
      if (!canonical && (meta.total > bufferConstants.MAX_STRING_LENGTH || meta.total > bufferConstants.MAX_LENGTH) || bytes.length > frame)
        throw harnessError("LIMIT_EXCEEDED", "Node fragment exceeds runtime capacity");
      if (meta.offset >= meta.total || meta.offset % frame || bytes.length !== Math.min(frame, meta.total - meta.offset))
        throw harnessError("INVALID_ARGUMENT", "Invalid node fragment range");
      const now = Date.now();
      for (const [key, entry] of this.#entries) if (entry.expires <= now) await this.#remove(key, entry);
      const key = JSON.stringify([owner.domainID, owner.principalID, meta.transferID]);
      let entry = this.#entries.get(key);
      if (!entry) {
        if (meta.offset !== 0) throw harnessError("INVALID_ARGUMENT", "Node transfer must begin at offset zero");
        if (this.#entries.size >= 64 || meta.total > this.quota - this.#reserved)
          throw harnessError("LIMIT_EXCEEDED", "Node fragment staging is full");
        const path = join(this.directory, randomUUID());
        this.lease.reserve(meta.total);
        try { const file = await open(path, "wx", 0o600); await file.close(); }
        catch (error) { this.lease.release(meta.total); throw error; }
        entry = { path, total: meta.total, digest: meta.digest, offset: 0, expires: now + this.ttlMs,
          hash: createHash("sha256"), complete: false, failed: false };
        this.#entries.set(key, entry); this.#reserved += meta.total;
      }
      if (entry.total !== meta.total || entry.digest !== meta.digest) throw harnessError("REQUEST_CONFLICT", "Node transfer identity is bound to different content");
      if (entry.failed) throw harnessError("INVALID_ARGUMENT", "Node transfer failed integrity validation");
      entry.expires = now + this.ttlMs;
      const file = await open(entry.path, "r+");
      try {
        if (meta.offset < entry.offset) {
          const previous = Buffer.alloc(bytes.length);
          const read = await file.read(previous, 0, previous.length, meta.offset);
          if (read.bytesRead !== previous.length || !timingSafeEqual(previous, bytes)) throw harnessError("REQUEST_CONFLICT", "Node fragment replay differs from the original");
        } else {
          if (meta.offset !== entry.offset || entry.complete) throw harnessError("INVALID_ARGUMENT", "Node fragments must be contiguous");
          let written = 0;
          while (written < bytes.length) {
            const result = await file.write(bytes, written, bytes.length - written, meta.offset + written);
            if (!result.bytesWritten) throw new Error("Node fragment write made no progress");
            written += result.bytesWritten;
          }
          entry.hash.update(bytes); entry.offset += bytes.length;
          if (entry.offset === entry.total) {
            if (entry.hash.digest("hex") !== entry.digest) { entry.failed = true; throw harnessError("INVALID_ARGUMENT", "Node transfer digest does not match"); }
            entry.complete = true;
          }
        }
      } finally { await file.close(); }
      assertCurrent();
      if (!entry.complete) return { complete: false, nextOffset: entry.offset };
      if (canonical && meta.offset + bytes.length < entry.total) return { complete: false, nextOffset: meta.offset + bytes.length };
      if (canonical) {
        const decoder = new InternalCanonicalJsonDecoder(), verification = createHash("sha256");
        const source = await open(entry.path, "r");
        try {
          assertCurrent(); let position = 0;
          const readPart = async (length: number, offset: number) => {
            const part = Buffer.allocUnsafe(length); let used = 0;
            while (used < length) { const read = await source.read(part, used, length - used, offset + used); assertCurrent(); if (!read.bytesRead) throw harnessError("INVALID_ARGUMENT", "Staged canonical node request is incomplete"); used += read.bytesRead; }
            return part;
          };
          while (position < entry.total) {
            const part = await readPart(Math.min(65_536, entry.total - position), position);
            verification.update(part); decoder.write(part); position += part.length;
            await new Promise<void>(resolve => setImmediate(resolve)); assertCurrent();
          }
          if (verification.digest("hex") !== entry.digest) throw harnessError("INVALID_ARGUMENT", "Staged canonical node request integrity changed");
          const value = decoder.finish(); position = 0;
          // The private incremental parser is restricted to canonical JSON.
          // Authenticate every original byte against canonical re-encoding.
          for (const expected of canonicalJsonChunks(value)) {
            const actual = await readPart(expected.length, position);
            if (!actual.equals(Buffer.from(expected))) throw harnessError("INVALID_ARGUMENT", "Noncanonical node request encoding");
            position += actual.length; await new Promise<void>(resolve => setImmediate(resolve)); assertCurrent();
          }
          if (position !== entry.total) throw harnessError("INVALID_ARGUMENT", "Canonical node request length changed");
          return { complete: true, value };
        } catch (error) { if (error instanceof HarnessError && error.info.code === "INVALID_ARGUMENT") entry.failed = true; throw error; }
        finally { await source.close(); }
      }
      const complete = await readFile(entry.path);
      // Verify the actual bytes that will be dispatched, not only the bytes
      // originally passed to write(). Yield while hashing large values.
      const verification = createHash("sha256");
      for (let position = 0; position < complete.length; position += 1_048_576) {
        verification.update(complete.subarray(position, position + 1_048_576));
        if (position % 8_388_608 === 0) await new Promise<void>(resolve => setImmediate(resolve));
      }
      if (complete.length !== entry.total || verification.digest("hex") !== entry.digest) {
        entry.failed = true;
        throw harnessError("INVALID_ARGUMENT", "Staged node transfer failed integrity validation");
      }
      return { complete: true, bytes: complete };
    }).finally(() => { this.#queued--; });
  }

  async close(): Promise<void> {
    await this.#serial(async () => {
      this.#closed = true;
      try { for (const [key, entry] of this.#entries) await this.#remove(key, entry); }
      finally { this.lease.close(); }
    });
  }

  async #remove(key: string, entry: Entry): Promise<void> {
    await unlink(entry.path).catch(error => { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; });
    this.lease.release(entry.total);
    this.#entries.delete(key); this.#reserved -= entry.total;
  }
  #serial<T>(operation: () => Promise<T>): Promise<T> {
    const current = this.#tail.then(operation);
    this.#tail = current.then(() => {}, () => {});
    return current;
  }
}
