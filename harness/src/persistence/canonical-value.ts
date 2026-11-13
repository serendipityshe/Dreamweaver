import { createHash, timingSafeEqual } from "node:crypto";
import { canonicalJsonChunks, harnessError } from "../contract/index.js";
import type { SQLiteStore, ValuePublication } from "./sqlite.js";
import { ValueStore } from "./value-store.js";

/** Compute the exact canonical UTF-8 identity without one giant JSON string. */
export async function canonicalValueWitness(value: unknown): Promise<{ length: number; digest: string }> {
  const hash = createHash("sha256"); let length = 0;
  for (const chunk of canonicalJsonChunks(value)) {
    length += chunk.byteLength;
    if (!Number.isSafeInteger(length)) throw new Error("Value is too large to identify");
    hash.update(chunk);
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  return { length, digest: hash.digest("hex") };
}

/** Compare a caller's native value against the actual immutable content, not
 * just an asserted digest. The source remains authoritative throughout the
 * bounded read, including across a concurrent deletion or epoch change. */
export async function verifyCanonicalValue(store: SQLiteStore, ref: ValuePublication, value: unknown, assertCurrent: () => void = () => {}): Promise<void> {
  store.assertAuthority();
  assertCurrent();
  const epoch = store.coordinatorEpoch, values = new ValueStore(store, Number.MAX_SAFE_INTEGER);
  const check = () => {
    const current = values.status(ref.id, ref.owner);
    if (current.state !== "sealed" && current.state !== "published" || current.encoding !== ref.encoding
        || current.length !== ref.length || current.digest !== ref.digest || current.written !== ref.length)
      throw harnessError("INVALID_ARGUMENT", "Staged value manifest does not match");
  };
  check();
  const chunks = canonicalJsonChunks(value)[Symbol.iterator](), digest = createHash("sha256");
  let expected = chunks.next(), expectedOffset = 0, length = 0;
  for await (const chunk of values.read(ref.id, ref.owner)) {
    assertCurrent();
    let offset = 0;
    while (offset < chunk.byteLength) {
      if (expected.done) throw harnessError("INVALID_ARGUMENT", "Staged value does not match its canonical source");
      const count = Math.min(chunk.byteLength - offset, expected.value.byteLength - expectedOffset);
      if (!timingSafeEqual(expected.value.subarray(expectedOffset, expectedOffset + count), chunk.subarray(offset, offset + count)))
        throw harnessError("INVALID_ARGUMENT", "Staged value does not match its canonical source");
      offset += count; expectedOffset += count;
      if (expectedOffset === expected.value.byteLength) { expected = chunks.next(); expectedOffset = 0; }
    }
    digest.update(chunk); length += chunk.byteLength;
    if (!Number.isSafeInteger(length)) throw harnessError("INVALID_ARGUMENT", "Staged value length is invalid");
  }
  if (!expected.done || length !== ref.length || digest.digest("hex") !== ref.digest) throw harnessError("INVALID_ARGUMENT", "Staged value integrity failed");
  store.assertAuthority();
  assertCurrent();
  if (store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Staged value authority changed");
  check();
}
