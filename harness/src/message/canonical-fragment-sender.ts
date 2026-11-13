import { createHash, randomUUID } from "node:crypto";
import { harnessError } from "../contract/index.js";

/** A replayable canonical byte snapshot, prepared once by canonicalJsonChunks.
 * The sender keeps only one frame; the caller owns the immutable snapshot. */
export class CanonicalFragmentSender {
  readonly transferID: string;
  readonly total: number;
  readonly digest: string;
  readonly #source: Iterable<Uint8Array>;
  #iterator: Iterator<Uint8Array>;
  #position = 0;
  #last: { offset: number; bytes: Uint8Array } | undefined;

  private constructor(source: Iterable<Uint8Array>, total: number, digest: string, id: string) {
    this.#source = source; this.total = total; this.digest = digest; this.transferID = id;
    this.#iterator = source[Symbol.iterator]();
  }

  static async create(source: Iterable<Uint8Array>, maxBytes: number, id: string = randomUUID(), assertCurrent: () => void = () => {}): Promise<CanonicalFragmentSender> {
    if (!source || typeof source[Symbol.iterator] !== "function" || !Number.isSafeInteger(maxBytes) || maxBytes <= 0
        || !/^[A-Za-z0-9_-]{1,128}$/.test(id)) throw harnessError("INVALID_ARGUMENT", "Invalid canonical fragment source");
    const hash = createHash("sha256"); let total = 0, chunks = 0;
    for (const chunk of source) {
      assertCurrent();
      if (!(chunk instanceof Uint8Array) || !chunk.length || chunk.length > 65_536) throw harnessError("INVALID_ARGUMENT", "Invalid canonical fragment chunk");
      if (chunk.length > maxBytes - total) throw harnessError("LIMIT_EXCEEDED", "Canonical fragment exceeds its byte budget");
      total += chunk.length; hash.update(chunk);
      if (++chunks % 16 === 0) await new Promise<void>(resolve => setImmediate(resolve));
    }
    assertCurrent();
    if (!total) throw harnessError("INVALID_ARGUMENT", "Empty canonical fragment source");
    return new CanonicalFragmentSender(source, total, hash.digest("hex"), id);
  }

  /** Frames are contiguous 64 KiB except the last. Retrying the most recent
   * offset returns identical bytes without advancing. Restart at zero is
   * supported for a receiver that has lost its transient staging. */
  fragment(offset: number): { offset: number; total: number; digest: string; transferID: string; bytes: Uint8Array } {
    if (!Number.isSafeInteger(offset) || offset < 0 || offset >= this.total || offset % 65_536 !== 0)
      throw harnessError("INVALID_ARGUMENT", "Invalid canonical fragment offset");
    if (this.#last?.offset === offset) return this.result(offset, this.#last.bytes);
    if (offset === 0 && this.#position !== 0) { this.#iterator = this.#source[Symbol.iterator](); this.#position = 0; }
    if (offset !== this.#position) throw harnessError("INVALID_ARGUMENT", "Canonical fragment offset is not contiguous");
    const next = this.#iterator.next(), expected = Math.min(65_536, this.total - offset);
    if (next.done || !(next.value instanceof Uint8Array) || next.value.byteLength !== expected)
      throw harnessError("INTERNAL_ERROR", "Canonical fragment source changed after its digest");
    const bytes = Uint8Array.from(next.value);
    this.#position += bytes.byteLength; this.#last = { offset, bytes };
    return this.result(offset, bytes);
  }

  private result(offset: number, bytes: Uint8Array) {
    return { offset, total: this.total, digest: this.digest, transferID: this.transferID, bytes: Uint8Array.from(bytes) };
  }
}
