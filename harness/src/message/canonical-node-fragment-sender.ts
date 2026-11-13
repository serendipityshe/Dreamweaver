import { canonicalJsonChunks, harnessError } from "../contract/index.js";
import { CanonicalFragmentSender } from "./canonical-fragment-sender.js";
import type { NodeFragmentMeta } from "./node-fragments.js";

/** Adapt the existing canonical byte source to the existing v2 1 MiB frames.
 * Keep one replay frame; never materialize the complete encoded request. */
export class CanonicalNodeFragmentSender {
  #last: { offset: number; bytes: Buffer } | undefined;
  private constructor(private readonly sender: CanonicalFragmentSender) {}
  static async create(value: unknown, maxBytes: number, assertCurrent: () => void = () => {}): Promise<CanonicalNodeFragmentSender> {
    return new CanonicalNodeFragmentSender(await CanonicalFragmentSender.create(canonicalJsonChunks(value), maxBytes, undefined, assertCurrent));
  }
  get transferID(): string { return this.sender.transferID; }
  get total(): number { return this.sender.total; }
  get digest(): string { return this.sender.digest; }
  fragment(offset: number): { meta: NodeFragmentMeta; bytes: Buffer } {
    if (!Number.isSafeInteger(offset) || offset < 0 || offset >= this.total || offset % 1_048_576) throw harnessError("INVALID_ARGUMENT", "Invalid canonical node fragment offset");
    if (this.#last?.offset !== offset) {
      const bytes = Buffer.allocUnsafe(Math.min(1_048_576, this.total - offset));
      for (let position = 0; position < bytes.length;) { const part = this.sender.fragment(offset + position).bytes; bytes.set(part, position); position += part.length; }
      this.#last = { offset, bytes };
    }
    return { meta: { transferID: this.transferID, offset, total: this.total, digest: this.digest }, bytes: Buffer.from(this.#last.bytes) };
  }
}
