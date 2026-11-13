import { harnessError } from "../contract/index.js";

export interface DurableReportChunks {
  key: string;
  length: number;
  digest: string;
  read(offset: number): Promise<Uint8Array>;
}

/** Reads one bounded frame at a time from an immutable ready node spool.
 * transferID must be restored from durable source identity, not randomized on
 * retry. The receiver verifies the complete digest before publication. */
export class DurableReportFragmentSender {
  readonly total: number;
  readonly digest: string;
  constructor(readonly source: DurableReportChunks, readonly transferID: string) {
    if (!source || typeof source.key !== "string" || !source.key || !Number.isSafeInteger(source.length) || source.length <= 0
        || typeof source.digest !== "string" || !/^[a-f0-9]{64}$/.test(source.digest) || typeof source.read !== "function"
        || !/^[A-Za-z0-9_-]{1,128}$/.test(transferID)) throw harnessError("INVALID_ARGUMENT", "Invalid durable report fragment source");
    this.total = source.length; this.digest = source.digest;
  }

  async fragment(offset: number): Promise<{ offset: number; total: number; digest: string; transferID: string; bytes: Uint8Array }> {
    if (!Number.isSafeInteger(offset) || offset < 0 || offset >= this.total || offset % 65_536)
      throw harnessError("INVALID_ARGUMENT", "Invalid durable report fragment offset");
    const bytes = await this.source.read(offset), expected = Math.min(65_536, this.total - offset);
    if (!(bytes instanceof Uint8Array) || bytes.length !== expected) throw harnessError("INTERNAL_ERROR", "Durable report chunk is unavailable");
    return { offset, total: this.total, digest: this.digest, transferID: this.transferID, bytes: Uint8Array.from(bytes) };
  }
}
