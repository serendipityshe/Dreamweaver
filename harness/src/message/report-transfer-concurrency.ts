import { harnessError } from "../contract/index.js";

/** Shared bounded admission for private report transfer decoding and writes. */
export class ReportTransferConcurrency {
  #active = 0;
  readonly #pending: Array<() => void> = [];
  constructor(readonly maxConcurrent = 2, readonly maxPending = 64) {
    if (!Number.isSafeInteger(maxConcurrent) || maxConcurrent < 1 || !Number.isSafeInteger(maxPending) || maxPending < 0)
      throw harnessError("INVALID_ARGUMENT", "Invalid report transfer concurrency");
  }
  async run<T>(operation: () => Promise<T>): Promise<T> {
    if (this.#active >= this.maxConcurrent) {
      if (this.#pending.length >= this.maxPending) throw harnessError("LIMIT_EXCEEDED", "Report transfer is busy");
      await new Promise<void>(resolve => this.#pending.push(resolve));
    } else this.#active++;
    try { return await operation(); }
    finally {
      const next = this.#pending.shift();
      if (next) next(); else this.#active--;
    }
  }
}
