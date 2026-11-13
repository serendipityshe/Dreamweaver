import { HarnessError, harnessError, InternalCanonicalJsonDecoder, parseJson, validateErrorInfo, type SessionBinding } from "../contract/index.js";
import { canonicalJsonChunks } from "../contract/index.js";
import { createHash } from "node:crypto";

interface Frame {
  transferID: string; offset: number; total: number; digest: string; bytes: Uint8Array;
}
interface FrameSender { fragment(offset: number): Frame | Promise<Frame> }

/** Private v4 test client. Callers own the persistent source identity and
 * retry decisions; a missing acknowledgement is always uncertain. */
export class BoundReportTransferClient {
  readonly #base: URL;
  #nodeHandle: Promise<string> | undefined;
  #workerDigest: Promise<string> | undefined;
  constructor(endpoint: string, readonly tokenProvider: () => string | Promise<string>, readonly nodeID: string, readonly binding: SessionBinding) {
    try { this.#base = new URL(endpoint); } catch { throw harnessError("INVALID_ARGUMENT", "Invalid transfer endpoint"); }
    if (!["http:", "https:"].includes(this.#base.protocol) || this.#base.username || this.#base.password || this.#base.search || this.#base.hash
        || this.#base.pathname !== "/" || typeof tokenProvider !== "function" || !nodeID || nodeID.trim() !== nodeID
        || !binding || typeof binding.domainID !== "string" || !binding.domainID || typeof binding.principalID !== "string" || !binding.principalID)
      throw harnessError("INVALID_ARGUMENT", "Invalid transfer configuration");
  }

  control(sender: FrameSender, offset: number, signal?: AbortSignal): Promise<{ nextOffset: number; bound: boolean }> {
    return this.send("control", sender, offset, undefined, signal) as Promise<{ nextOffset: number; bound: boolean }>;
  }

  report(sender: FrameSender, offset: number, final?: { expiresAt: number; observationOnly: boolean }, signal?: AbortSignal): Promise<{ nextOffset: number; stored: boolean; diagnostic?: true }> {
    return this.send("report", sender, offset, final, signal) as Promise<{ nextOffset: number; stored: boolean; diagnostic?: true }>;
  }

  private async send(kind: "control" | "report", sender: FrameSender, offset: number,
    final?: { expiresAt: number; observationOnly: boolean }, signal?: AbortSignal): Promise<{ nextOffset: number; bound?: boolean; stored?: boolean; diagnostic?: true }> {
    if (!sender || typeof sender.fragment !== "function" || signal !== undefined && !(signal instanceof AbortSignal)) throw harnessError("INVALID_ARGUMENT", "Invalid transfer request");
    const frame = await sender.fragment(offset);
    if (!frame || frame.offset !== offset || !Number.isSafeInteger(frame.total) || frame.total <= 0 || frame.offset >= frame.total
        || typeof frame.transferID !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(frame.transferID)
        || typeof frame.digest !== "string" || !/^[a-f0-9]{64}$/.test(frame.digest) || !(frame.bytes instanceof Uint8Array)
        || !frame.bytes.length || frame.bytes.length > 65_536 || frame.bytes.length > frame.total - frame.offset)
      throw harnessError("INVALID_ARGUMENT", "Invalid transfer frame");
    const isFinal = frame.offset + frame.bytes.length === frame.total;
    if (kind === "control" && final !== undefined || kind === "report" && isFinal !== (final !== undefined)
        || final && (!Number.isSafeInteger(final.expiresAt) || final.expiresAt <= 0 || typeof final.observationOnly !== "boolean"))
      throw harnessError("INVALID_ARGUMENT", "Invalid report finalization");
    const token = await this.tokenProvider();
    if (typeof token !== "string" || !/^[\x21-\x7e]+$/.test(token)) throw harnessError("UNAUTHENTICATED", "Credential unavailable");
    const target = new URL(this.#base); target.pathname = kind === "control" ? "/v4/report-control-fragment" : "/v4/bound-report-fragment";
    this.#nodeHandle ??= (async () => {
      const hash = createHash("sha256"); let count = 0;
      for (const part of canonicalJsonChunks(this.nodeID)) { hash.update(part); if (++count % 16 === 0) await new Promise<void>(resolve => setImmediate(resolve)); }
      return hash.digest("hex");
    })();
    const nodeHandle = await this.#nodeHandle;
    this.#workerDigest ??= (async () => {
      const hash = createHash("sha256"); let count = 0;
      for (const part of canonicalJsonChunks([this.binding.domainID, this.binding.principalID, nodeHandle])) {
        hash.update(part); if (++count % 16 === 0) await new Promise<void>(resolve => setImmediate(resolve));
      }
      return hash.digest("hex");
    })();
    const workerDigest = await this.#workerDigest;
    let response: Response;
    let verifiedRejection: HarnessError | undefined;
    try {
      response = await fetch(target, { method: "POST", signal, headers: { authorization: `Bearer ${token}`, "content-type": "application/octet-stream",
        "harness-fragment-version": "4", "harness-node-hash": nodeHandle, "harness-transfer-id": frame.transferID,
        "harness-fragment-offset": String(frame.offset), "harness-fragment-total": String(frame.total), "harness-fragment-sha256": frame.digest,
        ...(final ? { "harness-node-report-expires-at": String(final.expiresAt), "harness-node-report-observation-only": final.observationOnly ? "1" : "0" } : {}) },
        body: Uint8Array.from(frame.bytes) });
    } catch { throw harnessError("TRANSPORT_UNCERTAIN", "Report fragment acknowledgement is unavailable", null, "uncertain"); }
    try {
      if (response.headers.get("harness-v4-error-version") === "1") {
        verifiedRejection = await this.readErrorFragments(response, frame.transferID, workerDigest, signal);
        throw verifiedRejection;
      }
      if (!/^application\/json(?:\s*;|$)/i.test(response.headers.get("content-type") ?? "") || !response.body) throw new Error("Invalid acknowledgement");
      const parts: Uint8Array[] = []; let length = 0;
      for await (const part of response.body) { length += part.length; if (length > 2048) throw new Error("Acknowledgement exceeds limit"); parts.push(part); }
      const value = parseJson(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(Buffer.concat(parts)));
      if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid acknowledgement");
      if (!response.ok) {
        const compact = value.version === 4 && value.transferID === frame.transferID && value.workerDigest === workerDigest;
        const legacy = value.requestID === frame.transferID && value.binding && typeof value.binding === "object" && !Array.isArray(value.binding)
          && value.binding.domainID === this.binding.domainID && value.binding.principalID === this.binding.principalID;
        if (value.ok !== false || !Object.hasOwn(value, "error") || !compact && !legacy)
          throw new Error("Unbound transfer rejection");
        verifiedRejection = new HarnessError(validateErrorInfo(value.error));
        throw verifiedRejection;
      }
      const field = kind === "control" ? "bound" : "stored";
      const diagnostic = kind === "report" && isFinal && value.stored === false && value.diagnostic === true;
      if (Object.keys(value).sort().join(",") !== [field, "nextOffset", "transferID", "version", ...(diagnostic ? ["diagnostic"] : [])].sort().join(",")
          || value.version !== 4 || value.transferID !== frame.transferID || value.nextOffset !== frame.offset + frame.bytes.length
          || !diagnostic && value[field] !== isFinal) throw new Error("Unbound transfer acknowledgement");
      return { nextOffset: value.nextOffset as number, [field]: value[field] as boolean, ...(diagnostic ? { diagnostic: true as const } : {}) };
    } catch (error) {
      if (error === verifiedRejection) throw error;
      throw harnessError("TRANSPORT_UNCERTAIN", "Report fragment acknowledgement is invalid or incomplete", null, "uncertain");
    }
  }

  private async readErrorFragments(initial: Response, transferID: string, workerDigest: string, signal?: AbortSignal): Promise<HarnessError> {
    let id = "", total = 0, expectedDigest = "", offset = 0, frameBytes = 0, current = initial, first: Uint8Array | undefined;
    const hash = createHash("sha256"), decoder = new InternalCanonicalJsonDecoder();
    const fragmentRequest = async (request: { id: string; offset?: number; release?: true }): Promise<Response> => {
      const token = await this.tokenProvider();
      if (typeof token !== "string" || !/^[\x21-\x7e]+$/.test(token)) throw new Error("Credential unavailable");
      const target = new URL(this.#base); target.pathname = "/v4/error-fragment";
      return fetch(target, { method: "POST", signal, headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify(request) });
    };
    try {
      for (;;) {
        const rawID = current.headers.get("harness-v4-error-id"), rawOffset = current.headers.get("harness-v4-error-offset"),
          rawTotal = current.headers.get("harness-v4-error-total"), digest = current.headers.get("harness-v4-error-sha256");
        if (!current.ok || current.headers.get("harness-v4-error-version") !== "1" || !/^application\/octet-stream(?:\s*;|$)/i.test(current.headers.get("content-type") ?? "")
            || !rawID || !/^[0-9a-f-]{36}$/.test(rawID) || rawOffset === null || !/^(0|[1-9][0-9]*)$/.test(rawOffset)
            || !rawTotal || !/^[1-9][0-9]*$/.test(rawTotal) || !digest || !/^[a-f0-9]{64}$/.test(digest)
            || !Number.isSafeInteger(Number(rawTotal)) || Number(rawOffset) !== offset) throw new Error("Invalid error fragment");
        if (!id) { id = rawID; total = Number(rawTotal); expectedDigest = digest; }
        else if (id !== rawID || total !== Number(rawTotal) || expectedDigest !== digest) throw new Error("Error fragment identity changed");
        if (!current.body) throw new Error("Missing error fragment");
        const parts: Uint8Array[] = []; let length = 0;
        for await (const part of current.body) { length += part.length; if (length > 65_536 || offset + length > total) throw new Error("Error fragment exceeds budget"); parts.push(part); }
        if (!length) throw new Error("Empty error fragment");
        const bytes = Uint8Array.from(Buffer.concat(parts));
        if (!frameBytes) { frameBytes = length; first = bytes; }
        else if (length !== Math.min(frameBytes, total - offset)) throw new Error("Invalid error fragment size");
        hash.update(bytes); decoder.write(bytes); offset += bytes.length;
        if (offset === total) break;
        current = await fragmentRequest({ id, offset });
      }
      if (hash.digest("hex") !== expectedDigest) throw new Error("Error fragment digest mismatch");
      const value = decoder.finish();
      if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).sort().join(",") !== "error,ok,transferID,version,workerDigest"
          || value.version !== 4 || value.transferID !== transferID
          || value.workerDigest !== workerDigest || value.ok !== false || !Object.hasOwn(value, "error")) throw new Error("Unbound fragmented error");
      const confirmed = await fragmentRequest({ id, offset: 0 });
      if (!confirmed.ok || !/^application\/octet-stream(?:\s*;|$)/i.test(confirmed.headers.get("content-type") ?? "")
          || confirmed.headers.get("harness-v4-error-version") !== "1" || confirmed.headers.get("harness-v4-error-id") !== id
          || confirmed.headers.get("harness-v4-error-offset") !== "0" || confirmed.headers.get("harness-v4-error-total") !== String(total)
          || confirmed.headers.get("harness-v4-error-sha256") !== expectedDigest || !confirmed.body) throw new Error("Error authorization confirmation failed");
      const check: Uint8Array[] = []; let checked = 0;
      for await (const part of confirmed.body) { checked += part.length; if (checked > frameBytes) throw new Error("Invalid confirmation"); check.push(part); }
      if (!first || checked !== first.length || !Buffer.from(first).equals(Buffer.concat(check))) throw new Error("Error confirmation differs");
      const error = new HarnessError(validateErrorInfo(value.error));
      void fragmentRequest({ id, release: true }).catch(() => {});
      return error;
    } catch (error) {
      if (id) void fragmentRequest({ id, release: true }).catch(() => {});
      throw error;
    }
  }
}
