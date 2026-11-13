/** Local conservative lifetime of one already granted business context. */
export class LeaseClock {
  readonly #abort = new AbortController();
  #deadline: number;
  #connected = true;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #revision = 0;

  constructor(sentAt: number, ttlMs: number, readonly marginMs: number,
    readonly now: () => number = () => performance.now()) {
    this.#deadline = deadline(sentAt, ttlMs, marginMs);
    if (sentAt > this.now()) throw new RangeError("Lease send time is in the future");
    this.#schedule();
  }

  get signal(): AbortSignal { return this.#abort.signal; }

  /** Used at each managed-operation boundary, including after an await. */
  canOperate(): boolean {
    this.#expire();
    return this.#connected && !this.signal.aborted;
  }

  disconnect(): void { this.#connected = false; this.#revision++; this.#expire(); }

  /** A confirmed renewal uses its original send time; a late response cannot revive a context. */
  confirmRenewal(sentAt: number, ttlMs: number): boolean {
    this.#expire();
    if (this.signal.aborted) return false;
    const next = deadline(sentAt, ttlMs, this.marginMs);
    if (sentAt > this.now()) throw new RangeError("Lease send time is in the future");
    if (next <= this.now()) { this.abort(); return false; }
    this.#deadline = next;
    this.#schedule();
    return true;
  }

  /** The caller verifies the original identity, current authorization and cancellation remotely. */
  async reconnect(verify: () => Promise<boolean>): Promise<boolean> {
    this.#expire();
    if (this.signal.aborted) return false;
    const revision = ++this.#revision;
    const valid = await verify();
    this.#expire();
    if (revision !== this.#revision || this.signal.aborted) return false;
    if (!valid) { this.abort(); return false; }
    this.#connected = true;
    return true;
  }

  abort(): void {
    this.#connected = false;
    this.#revision++;
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#abort.abort();
  }

  #expire(): void { if (!this.signal.aborted && this.now() >= this.#deadline) this.abort(); }
  #schedule(): void {
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#expire();
    if (this.signal.aborted) return;
    this.#timer = setTimeout(() => this.#schedule(), Math.min(Math.max(0, this.#deadline - this.now()), 2_147_483_647));
    this.#timer.unref();
  }
}

function deadline(sentAt: number, ttlMs: number, marginMs: number): number {
  if (![sentAt, ttlMs, marginMs].every(Number.isFinite) || sentAt < 0 || ttlMs <= 0 || marginMs < 0 || marginMs >= ttlMs
      || !Number.isSafeInteger(ttlMs) || !Number.isSafeInteger(marginMs) || !Number.isFinite(sentAt + ttlMs)) {
    throw new RangeError("Invalid lease timing");
  }
  return sentAt + ttlMs - marginMs;
}
