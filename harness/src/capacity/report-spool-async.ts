import { Worker } from "node:worker_threads";
import { createHash } from "node:crypto";
import { canonicalJsonChunks, copyJson, harnessError, HarnessError, InternalCanonicalJsonDecoder, type ActionReport, type Json, type SessionBinding } from "../contract/index.js";
import type { SpoolEntry, SpoolHead, SpoolHeadDescriptor, SpoolSource } from "./report-spool.js";

/** Serial worker ownership keeps synchronous SQLite and JSON work off the node loop. */
export class AsyncReportSpool {
  readonly #worker: Worker;
  readonly #pending = new Map<number, { resolve(value: unknown): void; reject(error: unknown): void }>();
  readonly #opened: Promise<unknown>;
  readonly #identity: { binding: SessionBinding; nodeID: string };
  #sequence = 0;
  #failed: Error | undefined;

  constructor(directory: string, maxBytes: number, binding: SessionBinding, nodeID: string) {
    this.#identity = copyJson({ binding, nodeID }) as unknown as { binding: SessionBinding; nodeID: string };
    const filename = import.meta.url.endsWith(".ts") ? "./report-spool-worker.ts" : "./report-spool-worker.js";
    this.#worker = new Worker(new URL(filename, import.meta.url), import.meta.url.endsWith(".ts") ? { execArgv: ["--import", "tsx"] } : {});
    this.#worker.on("message", (message: { id: number; ok: boolean; result?: unknown; info?: ConstructorParameters<typeof HarnessError>[0] }) => {
      const pending = this.#pending.get(message.id); if (!pending) return;
      this.#pending.delete(message.id);
      if (!this.#pending.size) this.#worker.unref();
      if (message.ok) pending.resolve(message.result);
      else pending.reject(message.info ? new HarnessError(message.info) : new Error("Report spool operation failed"));
    });
    const failed = () => { this.#failed = new Error("Report spool worker is unavailable"); for (const pending of this.#pending.values()) pending.reject(this.#failed); this.#pending.clear(); };
    this.#worker.on("error", failed); this.#worker.on("exit", failed);
    this.#worker.unref();
    const chunks = canonicalJsonChunks(this.#identity); let bytes = 0;
    for (const chunk of chunks) { bytes += chunk.length; if (bytes > 65_536) break; }
    this.#opened = bytes <= 65_536 ? this.#call("open", [directory, maxBytes, this.#identity.binding, this.#identity.nodeID]) : (async () => {
      await this.#call("openIdentityStart", [directory, maxBytes]);
      for (const chunk of chunks) { const data = Uint8Array.from(chunk); await this.#call("openIdentityChunk", [data], [data.buffer]); }
      return this.#call("openIdentityFinish", []);
    })();
  }
  #call(method: string, args: unknown[], transfer: ArrayBuffer[] = []): Promise<unknown> {
    if (this.#failed) return Promise.reject(this.#failed);
    const id = ++this.#sequence;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#worker.ref();
      try { this.#worker.postMessage({ id, method, args }, transfer); } catch (error) { this.#pending.delete(id); if (!this.#pending.size) this.#worker.unref(); reject(error); }
    });
  }
  async #invoke(method: string, args: unknown[], transfer: ArrayBuffer[] = []): Promise<unknown> { await this.#opened; return this.#call(method, args, transfer); }
  async #hashUpdate(ids: string[], bytes: Uint8Array): Promise<void> {
    const data = Uint8Array.from(bytes);
    await this.#invoke("hashUpdate", [ids, data], [data.buffer]);
  }
  async #abortHashes(ids: string[]): Promise<void> {
    for (const id of ids) await this.#invoke("hashAbort", [id]).catch(() => {});
  }
  async validate(binding: SessionBinding, nodeID: string): Promise<void> {
    await this.#opened;
    if (!binding || binding.domainID !== this.#identity.binding.domainID || binding.principalID !== this.#identity.binding.principalID || nodeID !== this.#identity.nodeID)
      throw harnessError("SESSION_MISMATCH", "Report spool belongs to another node or principal");
    await this.#call("validate", []);
  }
  async measure(value: Json, maxBytes: number): Promise<{ oversized: boolean; digest?: string }> {
    if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) throw harnessError("INVALID_ARGUMENT", "Expected a positive safe maxValueBytes");
    const chunks = canonicalJsonChunks(value), [id] = await this.#invoke("hashStart", [1]) as [string];
    try {
      let bytes = 0;
      for (const chunk of chunks) {
        bytes += chunk.byteLength;
        if (!Number.isSafeInteger(bytes)) throw new Error("Report size exceeds the supported integer range");
        await this.#hashUpdate([id], chunk);
      }
      const digest = await this.#invoke("hashFinish", [id]) as string;
      return bytes > maxBytes ? { oversized: true, digest } : { oversized: false };
    } finally { await this.#abortHashes([id]); }
  }
  async reserve(source: SpoolSource, maxBytes: number): Promise<string> {
    const snapshot = copyJson(source) as unknown as SpoolSource, chunks = canonicalJsonChunks(snapshot);
    let size = 0;
    for (const part of chunks) { size += part.length; if (size > 65_536) break; }
    if (size <= 65_536) return this.#invoke("reserve", [snapshot, maxBytes]) as Promise<string>;
    const ids = await this.#invoke("hashStart", [2]) as [string, string];
    let sourceBytes = 0, sourceDigest: string, identityDigest: string;
    try {
      for (const part of chunks) { sourceBytes += part.length; if (!Number.isSafeInteger(sourceBytes)) throw new Error("Source size exceeds the supported integer range"); await this.#hashUpdate([ids[0]], part); }
      for (const part of canonicalJsonChunks([snapshot.attemptID, snapshot.phase])) await this.#hashUpdate([ids[1]], part);
      sourceDigest = await this.#invoke("hashFinish", [ids[0]]) as string;
      identityDigest = await this.#invoke("hashFinish", [ids[1]]) as string;
    } finally { await this.#abortHashes(ids); }
    const key = `v2:${identityDigest}`;
    await this.#invoke("beginChunkedSource", [key, sourceBytes, sourceDigest, maxBytes]);
    try {
      let offset = 0;
      for (const part of chunks) {
        const data = Uint8Array.from(part);
        await this.#invoke("appendSourceChunk", [key, offset, data], [data.buffer]); offset += part.length;
      }
      await this.#invoke("finishChunkedSource", [key]);
      return key;
    } catch (error) {
      await this.#invoke("abortChunkedSource", [key]).catch(() => {});
      throw error;
    }
  }
  async save(key: string, source: SpoolSource, report: ActionReport): Promise<void> {
    const sourceSnapshot = copyJson(source) as unknown as SpoolSource;
    const chunks = canonicalJsonChunks(report), sourceChunks = canonicalJsonChunks(sourceSnapshot), ids: string[] = [];
    let length = 0, sourceLength = 0, digest: string, bodyDigest: string;
    try {
      ids.push(...await this.#invoke("hashStart", [2]) as [string, string]);
      for (const chunk of sourceChunks) { sourceLength += chunk.length; await this.#hashUpdate([ids[0]!], chunk); }
      await this.#hashUpdate([ids[0]!], Uint8Array.of(0));
      for (const chunk of chunks) {
        length += chunk.byteLength;
        if (!Number.isSafeInteger(length)) throw new Error("Report size exceeds the supported integer range");
        await this.#hashUpdate(ids, chunk);
      }
      digest = await this.#invoke("hashFinish", [ids[0]]) as string;
      bodyDigest = await this.#invoke("hashFinish", [ids[1]]) as string;
    } finally { await this.#abortHashes(ids); }
    // Keep old single-row records readable and avoid changing the existing
    // fault-injection/commit boundary for small reports.
    const streamedSource = key.startsWith("v2:");
    if (length <= 65_536 && !streamedSource) { await this.#invoke("save", [key, sourceSnapshot, report]); return; }
    await this.#invoke("checkChunkReservation", [key, streamedSource ? sourceLength : sourceSnapshot, length]);
    if (streamedSource) {
      const id = await this.#invoke("startSourceComparison", [key, sourceSnapshot.grant.expiresAt]) as string;
      try {
        for (const part of sourceChunks) {
          const data = Uint8Array.from(part);
          await this.#invoke("compareSourceChunk", [id, data], [data.buffer]);
        }
        await this.#invoke("finishSourceComparison", [id]);
      } finally { await this.#invoke("abortSourceComparison", [id]).catch(() => {}); }
    }
    let offset = 0;
    for (const chunk of chunks) {
      const data = Uint8Array.from(chunk);
      await this.#invoke("appendChunk", [key, offset, data], [data.buffer]);
      offset += chunk.byteLength;
    }
    await this.#invoke("finishChunks", [key, streamedSource ? sourceSnapshot.grant.expiresAt : sourceSnapshot, length, digest, bodyDigest]);
  }
  /** Read-only view of an already durable, chunked report; no whole body is cloned. */
  async chunks(key: string): Promise<{ key: string; length: number; digest: string; read(offset: number): Promise<Uint8Array> }> {
    const metadata = await this.#invoke("describeChunks", [key]) as { length: number; digest: string };
    return { key, ...metadata, read: async offset => {
      if (!Number.isSafeInteger(offset) || offset < 0 || offset >= metadata.length) throw harnessError("INVALID_ARGUMENT", "Invalid spool read offset");
      const chunk = await this.#invoke("readChunk", [key, offset]);
      if (!(chunk instanceof Uint8Array) || !chunk.byteLength || chunk.byteLength > metadata.length - offset) throw new Error("Retained report chunk is unavailable");
      return chunk;
    } };
  }
  async remove(key: string): Promise<void> { await this.#invoke("remove", [key]); }
  async actionIDs(): Promise<string[]> {
    const keys = await this.#invoke("readyKeys", []) as string[], ids = new Set<string>();
    for (const key of keys) ids.add((await this.describe(key)).source.actionID);
    return [...ids];
  }
  async head(): Promise<SpoolHead | null> {
    const descriptor = await this.#invoke("headDescriptor", []) as SpoolHeadDescriptor | null;
    return descriptor ? this.#decodeHead(descriptor) : null;
  }
  async describe(key: string): Promise<SpoolHead> { return this.#decodeHead(await this.#invoke("describeDescriptor", [key]) as SpoolHeadDescriptor); }
  async #decodeHead(descriptor: SpoolHeadDescriptor): Promise<SpoolHead> {
    if (descriptor.kind === "inline") return descriptor.head;
    const decoder = new InternalCanonicalJsonDecoder(), hash = createHash("sha256"); let offset = 0;
    while (offset < descriptor.sourceBytes) {
      const bytes = await this.#invoke("readSourceChunk", [descriptor.key, offset]);
      if (!(bytes instanceof Uint8Array) || !bytes.length || bytes.length > 65_536 || bytes.length > descriptor.sourceBytes - offset)
        throw new Error("Retained report source chunk is unavailable");
      hash.update(bytes); decoder.write(bytes); offset += bytes.length;
    }
    if (hash.digest("hex") !== descriptor.sourceDigest) throw new Error("Retained report source checksum mismatch");
    const source = decoder.finish() as unknown as SpoolSource;
    if (!source || typeof source !== "object" || !source.grant || source.nodeID !== this.#identity.nodeID || source.grant.nodeID !== source.nodeID
        || source.grant.workerPrincipalID !== this.#identity.binding.principalID || source.grant.subjectID !== source.actionID
        || source.grant.attemptID !== source.attemptID || source.assignmentID !== source.attemptID || source.grant.incarnation !== source.incarnation
        || typeof source.runID !== "string" || !source.runID.trim() || !Number.isSafeInteger(source.grant.expiresAt) || source.grant.expiresAt <= 0)
      throw new Error("Retained report source identity is invalid");
    const identity = createHash("sha256"); for (const part of canonicalJsonChunks([source.attemptID, source.phase])) identity.update(part);
    if (`v2:${identity.digest("hex")}` !== descriptor.key) throw new Error("Retained report source key changed");
    let sourceLength = descriptor.sourceBytes;
    if (descriptor.sourceExpiresAt !== null) {
      sourceLength += String(descriptor.sourceExpiresAt).length - String(source.grant.expiresAt).length;
      source.grant.expiresAt = descriptor.sourceExpiresAt;
    }
    const length = descriptor.contentBytes - sourceLength;
    if (!Number.isSafeInteger(length) || length <= 0) throw new Error("Retained report length is invalid");
    return { key: descriptor.key, source, chunked: true, length, digest: descriptor.digest };
  }
  async next(): Promise<SpoolEntry | null> {
    const head = await this.head();
    if (!head || !("chunked" in head)) return head;
    const decoder = new InternalCanonicalJsonDecoder(), hash = createHash("sha256");
    for (const part of canonicalJsonChunks(head.source)) hash.update(part);
    hash.update("\0");
    let offset = 0;
    while (offset < head.length) {
      const bytes = await this.#invoke("readChunk", [head.key, offset]);
      if (!(bytes instanceof Uint8Array) || !bytes.byteLength || bytes.byteLength > head.length - offset) throw new Error("Retained report chunk is unavailable");
      hash.update(bytes); decoder.write(bytes); offset += bytes.byteLength;
    }
    if (hash.digest("hex") !== head.digest) throw new Error("Retained report checksum mismatch");
    return { key: head.key, source: head.source, report: decoder.finish() as unknown as ActionReport };
  }
  async close(): Promise<void> {
    try { await this.#invoke("close", []); }
    finally { await this.#worker.terminate(); }
  }
}
