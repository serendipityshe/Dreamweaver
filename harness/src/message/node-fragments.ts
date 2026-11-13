import { constants as bufferConstants } from "node:buffer";
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { harnessError } from "../contract/index.js";

/** Binary node-request fragments; metadata is not part of the business RPC. */
export interface NodeFragmentMeta { transferID: string; offset: number; total: number; digest: string }
/** These values must come from the current coordinator and authentication, not fragment headers. */
export interface NodeFragmentOwner { domainID: string; principalID: string }
export type NodeFragmentResult = { complete: false; nextOffset: number } | { complete: true; bytes: Buffer };
export interface NodeFragmentLimits {
  maxFrameBytes: number;
  maxTransferBytes: number;
  maxActiveTransfers: number;
  /** Caps per-transfer Map entries as well as payload bytes. Defaults to 4096. */
  maxFragments?: number;
  /** Includes the temporary full-size concatenation copy on completion. */
  maxReservedBytes: number;
  ttlMs: number;
  clock?: () => number;
}

const positive = (n: unknown): n is number => typeof n === "number" && Number.isSafeInteger(n) && n > 0;
const natural = (n: unknown): n is number => typeof n === "number" && Number.isSafeInteger(n) && n >= 0;
const transferID = (s: unknown): s is string => typeof s === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(s);
const digest = (s: unknown): s is string => typeof s === "string" && /^[a-f0-9]{64}$/.test(s);
const identity = (s: unknown): s is string => typeof s === "string" && !!s && s.trim() === s;

/** Sender owns an immutable copy so retries always use exactly the same bytes. */
export class NodeFragmentSender {
  readonly transferID: string;
  readonly digest: string;
  readonly #bytes: Buffer;
  constructor(body: Uint8Array, readonly maxFrameBytes: number, id: string = randomUUID()) {
    if (!(body instanceof Uint8Array) || !body.byteLength || !positive(maxFrameBytes) || !transferID(id)) throw harnessError("INVALID_ARGUMENT", "Invalid node fragment sender");
    this.#bytes = Buffer.from(body);
    this.transferID = id;
    this.digest = createHash("sha256").update(this.#bytes).digest("hex");
  }
  get total(): number { return this.#bytes.length; }
  fragment(offset: number): { meta: NodeFragmentMeta; bytes: Buffer } {
    if (!natural(offset) || offset >= this.total || offset % this.maxFrameBytes !== 0) throw harnessError("INVALID_ARGUMENT", "Invalid node fragment offset");
    const bytes = Buffer.from(this.#bytes.subarray(offset, Math.min(offset + this.maxFrameBytes, this.total)));
    return { meta: { transferID: this.transferID, offset, total: this.total, digest: this.digest }, bytes };
  }
}

interface Transfer {
  total: number;
  digest: string;
  expiresAt: number;
  offset: number;
  parts: Map<number, Buffer>;
  hash: ReturnType<typeof createHash>;
  complete?: Buffer;
  failed?: boolean;
}

/**
 * Transient, per-coordinator staging. Complete bytes are never dispatched by
 * this class: the caller must reauthenticate, parse, validate the full RPC,
 * and recheck its original session/assignment/lease before any mutation.
 * A restart loses staging; the sender retries the same semantic RPC identity.
 */
export class NodeFragmentReceiver {
  readonly #limits: Required<NodeFragmentLimits>;
  readonly #transfers = new Map<string, Transfer>();
  #reserved = 0;
  #closed = false;
  constructor(limits: NodeFragmentLimits) {
    if (!limits || !positive(limits.maxFrameBytes) || !positive(limits.maxTransferBytes) || !positive(limits.maxActiveTransfers)
        || !positive(limits.maxReservedBytes) || !positive(limits.ttlMs) || limits.clock !== undefined && typeof limits.clock !== "function"
        || limits.maxFragments !== undefined && !positive(limits.maxFragments)
        || limits.maxTransferBytes > bufferConstants.MAX_STRING_LENGTH || limits.maxTransferBytes > bufferConstants.MAX_LENGTH) {
      throw harnessError("INVALID_ARGUMENT", "Invalid node fragment limits");
    }
    this.#limits = { ...limits, maxFragments: limits.maxFragments ?? 4096, clock: limits.clock ?? (() => Date.now()) };
  }

  accept(owner: NodeFragmentOwner, meta: NodeFragmentMeta, bytes: Uint8Array): NodeFragmentResult {
    if (this.#closed) throw harnessError("NODE_STATE_INVALID", "Node fragment receiver is closed");
    if (!owner || !identity(owner.domainID) || !identity(owner.principalID) || !meta || !transferID(meta.transferID)
        || !natural(meta.offset) || !positive(meta.total) || !digest(meta.digest) || !(bytes instanceof Uint8Array) || !bytes.byteLength) {
      throw harnessError("INVALID_ARGUMENT", "Invalid node fragment");
    }
    if (bytes.byteLength > this.#limits.maxFrameBytes || meta.total > this.#limits.maxTransferBytes) throw harnessError("LIMIT_EXCEEDED", "Node fragment exceeds the configured limit");
    if (Math.ceil(meta.total / this.#limits.maxFrameBytes) > this.#limits.maxFragments) throw harnessError("LIMIT_EXCEEDED", "Node transfer exceeds the fragment count limit");
    if (meta.offset >= meta.total || meta.offset % this.#limits.maxFrameBytes !== 0
        || bytes.byteLength !== Math.min(this.#limits.maxFrameBytes, meta.total - meta.offset)) throw harnessError("INVALID_ARGUMENT", "Invalid node fragment range");
    const now = this.#now();
    this.#expire(now);
    const key = JSON.stringify([owner.domainID, owner.principalID, meta.transferID]);
    let state = this.#transfers.get(key);
    if (!state) {
      if (meta.offset !== 0) throw harnessError("INVALID_ARGUMENT", "Node transfer must begin at offset zero");
      const reserve = meta.total * 2;
      if (!Number.isSafeInteger(reserve) || this.#transfers.size >= this.#limits.maxActiveTransfers
          || reserve > this.#limits.maxReservedBytes - this.#reserved) throw harnessError("LIMIT_EXCEEDED", "Node fragment staging is full");
      const expiresAt = now + this.#limits.ttlMs;
      if (!Number.isSafeInteger(expiresAt)) throw harnessError("INVALID_ARGUMENT", "Invalid node fragment expiry");
      state = { total: meta.total, digest: meta.digest, expiresAt, offset: 0, parts: new Map(), hash: createHash("sha256") };
      this.#transfers.set(key, state); this.#reserved += reserve;
    }
    if (state.total !== meta.total || state.digest !== meta.digest) throw harnessError("REQUEST_CONFLICT", "Node transfer identity is bound to different content");
    if (state.failed) throw harnessError("INVALID_ARGUMENT", "Node transfer failed integrity validation");
    if (meta.offset < state.offset) {
      const previous = state.parts.get(meta.offset);
      if (!previous || previous.length !== bytes.byteLength || !timingSafeEqual(previous, bytes)) throw harnessError("REQUEST_CONFLICT", "Node fragment replay differs from the original");
      return state.complete ? { complete: true, bytes: state.complete } : { complete: false, nextOffset: state.offset };
    }
    if (meta.offset !== state.offset || state.complete) throw harnessError("INVALID_ARGUMENT", "Node fragments must be contiguous");
    const copy = Buffer.from(bytes);
    state.parts.set(meta.offset, copy); state.hash.update(copy); state.offset += copy.length;
    if (state.offset !== state.total) return { complete: false, nextOffset: state.offset };
    if (state.hash.digest("hex") !== state.digest) {
      // Retain a bounded tombstone until expiry/release: the same transfer ID
      // must not silently bind to different content after an integrity error.
      state.failed = true; state.parts.clear();
      throw harnessError("INVALID_ARGUMENT", "Node transfer digest does not match");
    }
    state.complete = Buffer.concat([...state.parts.values()], state.total);
    return { complete: true, bytes: state.complete };
  }

  release(owner: NodeFragmentOwner, id: string): void {
    if (!owner || !identity(owner.domainID) || !identity(owner.principalID) || !transferID(id)) throw harnessError("INVALID_ARGUMENT", "Invalid node transfer identity");
    const key = JSON.stringify([owner.domainID, owner.principalID, id]), state = this.#transfers.get(key);
    if (state) this.#remove(key, state);
  }
  close(): void { this.#closed = true; this.#transfers.clear(); this.#reserved = 0; }
  #now(): number { const now = this.#limits.clock(); if (!natural(now)) throw harnessError("INVALID_ARGUMENT", "Invalid node fragment clock"); return now; }
  #expire(now: number): void { for (const [key, state] of this.#transfers) if (state.expiresAt <= now) this.#remove(key, state); }
  #remove(key: string, state: Transfer): void { this.#transfers.delete(key); this.#reserved -= state.total * 2; }
}
