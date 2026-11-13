import { parentPort } from "node:worker_threads";
import { createHash, randomUUID, type Hash } from "node:crypto";
import { assertJsonValueSize, canonicalJson, HarnessError, InternalCanonicalJsonDecoder, type Json, type SessionBinding } from "../contract/index.js";
import { ReportSpool, type SpoolSource } from "./report-spool.js";

type Request = { id: number; method: string; args: unknown[] };
let spool: ReportSpool | undefined;
let spoolIdentity: { binding: SessionBinding; nodeID: string } | undefined;
let opening: { directory: string; maxBytes: number; decoder: InternalCanonicalJsonDecoder } | undefined;
const hashes = new Map<string, Hash>();
let queue = Promise.resolve();
if (!parentPort) throw new Error("Report spool worker requires a parent port");
const port = parentPort;
port.on("message", (request: Request) => {
  queue = queue.then(async () => {
    try {
      const args = request.args;
      let result: unknown;
      if (request.method === "open") {
        if (spool || opening) throw new Error("Report spool is already open");
        spool = new ReportSpool(args[0] as string, args[1] as number, args[2] as SessionBinding, args[3] as string);
        spoolIdentity = { binding: args[2] as SessionBinding, nodeID: args[3] as string };
        result = null;
      } else if (request.method === "openIdentityStart") {
        if (spool || opening || typeof args[0] !== "string" || typeof args[1] !== "number") throw new Error("Invalid report spool opening");
        opening = { directory: args[0], maxBytes: args[1], decoder: new InternalCanonicalJsonDecoder() };
        result = null;
      } else if (request.method === "openIdentityChunk") {
        if (!opening || !(args[0] instanceof Uint8Array) || !args[0].byteLength || args[0].byteLength > 65_536) throw new Error("Invalid spool identity chunk");
        opening.decoder.write(args[0]); result = null;
      } else if (request.method === "openIdentityFinish") {
        if (!opening) throw new Error("Report spool identity is unavailable");
        const identity = opening.decoder.finish() as unknown as { binding: SessionBinding; nodeID: string };
        spool = new ReportSpool(opening.directory, opening.maxBytes, identity.binding, identity.nodeID);
        spoolIdentity = identity; opening = undefined;
        result = null;
      } else if (request.method === "measure") {
        try { assertJsonValueSize(args[0] as Json, args[1] as number); result = { oversized: false }; }
        catch (error) {
          if (!(error instanceof HarnessError) || error.info.code !== "LIMIT_EXCEEDED") throw error;
          result = { oversized: true, digest: createHash("sha256").update(canonicalJson(args[0])).digest("hex") };
        }
      } else if (request.method === "hashStart") {
        const count = args[0];
        if (count !== 1 && count !== 2) throw new Error("Invalid report digest reservation");
        if (hashes.size + count > 64) throw new Error("Too many pending report digests");
        const ids = Array.from({ length: count }, () => randomUUID());
        for (const id of ids) hashes.set(id, createHash("sha256"));
        result = ids;
      } else if (request.method === "hashUpdate") {
        const ids = args[0] as string[], data = args[1];
        if (!Array.isArray(ids) || !ids.length || !(data instanceof Uint8Array) || data.byteLength > 65_536) throw new Error("Invalid report digest update");
        const targets = ids.map(id => hashes.get(id));
        if (targets.some(hash => !hash)) throw new Error("Unknown report digest");
        for (const hash of targets) hash!.update(data);
        result = null;
      } else if (request.method === "hashFinish") {
        const id = args[0] as string, hash = hashes.get(id);
        if (!hash) throw new Error("Unknown report digest");
        hashes.delete(id); result = hash.digest("hex");
      } else if (request.method === "hashAbort") {
        hashes.delete(args[0] as string); result = null;
      } else {
        if (!spool) throw new Error("Report spool is unavailable");
        switch (request.method) {
          case "validate": if (!spoolIdentity) throw new Error("Report spool identity is unavailable"); result = await spool.validate(spoolIdentity.binding, spoolIdentity.nodeID); break;
          case "reserve": result = spool.reserve(args[0] as SpoolSource, args[1] as number); break;
          case "beginChunkedSource": result = spool.beginChunkedSource(args[0] as string, args[1] as number, args[2] as string, args[3] as number); break;
          case "appendSourceChunk": result = spool.appendSourceChunk(args[0] as string, args[1] as number, args[2] as Uint8Array); break;
          case "abortChunkedSource": result = spool.abortChunkedSource(args[0] as string); break;
          case "finishChunkedSource": result = spool.finishChunkedSource(args[0] as string); break;
          case "startSourceComparison": result = spool.startSourceComparison(args[0] as string, args[1] as number); break;
          case "compareSourceChunk": result = spool.compareSourceChunk(args[0] as string, args[1] as Uint8Array); break;
          case "finishSourceComparison": result = spool.finishSourceComparison(args[0] as string); break;
          case "abortSourceComparison": result = spool.abortSourceComparison(args[0] as string); break;
          case "save": result = spool.save(args[0] as string, args[1] as SpoolSource, args[2] as Parameters<ReportSpool["save"]>[2]); break;
          case "checkChunkReservation": result = spool.checkChunkReservation(args[0] as string, args[1] as SpoolSource | number, args[2] as number); break;
          case "appendChunk": result = spool.appendChunk(args[0] as string, args[1] as number, args[2] as Uint8Array); break;
          case "finishChunks": result = spool.finishChunks(args[0] as string, args[1] as SpoolSource | number, args[2] as number, args[3] as string, args[4] as string); break;
          case "head": result = spool.head(); break;
          case "headDescriptor": result = spool.headDescriptor(); break;
          case "describeDescriptor": result = spool.describeDescriptor(args[0] as string); break;
          case "readSourceChunk": result = spool.readSourceChunk(args[0] as string, args[1] as number); break;
          case "readyKeys": result = spool.readyKeys(); break;
          case "describe": result = spool.describe(args[0] as string); break;
          case "readChunk": result = spool.readChunk(args[0] as string, args[1] as number); break;
          case "describeChunks": result = spool.describeChunks(args[0] as string); break;
          case "remove": result = spool.remove(args[0] as string); break;
          case "actionIDs": result = spool.actionIDs(); break;
          case "next": result = spool.next(); break;
          case "close": spool.close(); spool = undefined; spoolIdentity = undefined; result = null; break;
          default: throw new Error("Unknown report spool operation");
        }
      }
      if (result instanceof Uint8Array) port.postMessage({ id: request.id, ok: true, result }, [result.buffer as ArrayBuffer]);
      else port.postMessage({ id: request.id, ok: true, result });
    } catch (error) {
      port.postMessage({ id: request.id, ok: false, info: error instanceof HarnessError ? error.info : undefined });
    }
  });
});
