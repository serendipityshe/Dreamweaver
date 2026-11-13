import { createHash, timingSafeEqual } from "node:crypto";
import { parentPort } from "node:worker_threads";
import { canonicalJsonChunks, InternalCanonicalJsonDecoder, type Json } from "../contract/index.js";

if (!parentPort) throw new Error("Report validator requires a worker port");
const port = parentPort, decoder = new InternalCanonicalJsonDecoder(), hash = createHash("sha256");
let read = 0, iterator: Iterator<Uint8Array> | undefined, expected: IteratorResult<Uint8Array> | undefined, expectedOffset = 0;
let state: "reading" | "comparing" | "done" = "reading";

function size(value: Json): number {
  let bytes = 0;
  for (const part of canonicalJsonChunks(value)) { bytes += part.byteLength; if (!Number.isSafeInteger(bytes)) throw new Error("Invalid report length"); }
  return bytes;
}

port.on("message", (message: { id: number; method: string; bytes?: Uint8Array }) => {
  try {
    let result: unknown = null;
    if (message.method === "read") {
      if (state !== "reading" || !(message.bytes instanceof Uint8Array) || !message.bytes.byteLength || message.bytes.byteLength > 65_536)
        throw new Error("Invalid report validation chunk");
      decoder.write(message.bytes); hash.update(message.bytes); read += message.bytes.byteLength;
      if (!Number.isSafeInteger(read)) throw new Error("Invalid report length");
    } else if (message.method === "finish") {
      if (state !== "reading") throw new Error("Invalid report validation state");
      const report = decoder.finish();
      if (!report || typeof report !== "object" || Array.isArray(report) || Object.keys(report).sort().join(",") !== "kind,value" || report.kind !== "report")
        throw new Error("Invalid report envelope");
      const body = report.value;
      if (body === undefined) throw new Error("Missing report value");
      const object = body && typeof body === "object" && !Array.isArray(body) ? body : null;
      result = { length: read, digest: hash.digest("hex"), valueBytes: size(body),
        payloadBytes: object && object.payload !== undefined ? size(object.payload) : null,
        evidenceBytes: object && object.evidence !== undefined ? size(object.evidence) : null };
      iterator = canonicalJsonChunks(report)[Symbol.iterator](); expected = iterator.next(); state = "comparing";
    } else if (message.method === "compare") {
      if (state !== "comparing" || !(message.bytes instanceof Uint8Array) || !message.bytes.byteLength || message.bytes.byteLength > 65_536 || !iterator || !expected)
        throw new Error("Invalid report comparison chunk");
      let offset = 0;
      while (offset < message.bytes.byteLength) {
        if (expected.done) throw new Error("Noncanonical report bytes");
        const count = Math.min(message.bytes.byteLength - offset, expected.value.byteLength - expectedOffset);
        if (!timingSafeEqual(expected.value.subarray(expectedOffset, expectedOffset + count), message.bytes.subarray(offset, offset + count)))
          throw new Error("Noncanonical report bytes");
        offset += count; expectedOffset += count;
        if (expectedOffset === expected.value.byteLength) { expected = iterator.next(); expectedOffset = 0; }
      }
    } else if (message.method === "verified") {
      if (state !== "comparing" || !expected?.done) throw new Error("Incomplete report comparison");
      state = "done";
    } else throw new Error("Unknown report validation operation");
    port.postMessage({ id: message.id, ok: true, result });
  } catch { port.postMessage({ id: message.id, ok: false }); }
});
