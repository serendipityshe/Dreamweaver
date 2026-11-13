import { constants as bufferConstants } from "node:buffer";
import { harnessError } from "./errors.js";
import type { Json } from "./types.js";

// This decoder is private to the canonical chunk protocol. In particular, a
// number must use JSON.stringify's canonical spelling: the legacy parser
// accepts arbitrarily long noncanonical number tokens and remains separate.
type ArrayFrame = { kind: "array"; value: Json[]; state: "first" | "value" | "separator" };
type ObjectFrame = { kind: "object"; value: { [key: string]: Json }; keys: Set<string>; key?: string; state: "first" | "key" | "colon" | "value" | "separator" };
type Frame = ArrayFrame | ObjectFrame;

function bad(message: string): never { throw harnessError("INVALID_ARGUMENT", message); }
const numeric = /^[\-0-9eE+.]+$/;
const whitespace = (char: string) => char === " " || char === "\n" || char === "\r" || char === "\t";

/** Incrementally builds native Json without joining the whole document.
 * Intended for our internal canonical encoding; it is not itself a full
 * canonical-byte or provenance verifier. If decoding untrusted input, compare
 * all original bytes with canonical re-encoding and authenticate the source
 * before using the resulting value or publishing any business state.
 * Individual strings still need to fit the host runtime. */
export class InternalCanonicalJsonDecoder {
  readonly #decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  readonly #stack: Frame[] = [];
  #root: Json | undefined;
  #done = false;
  #closed = false;
  #failed = false;
  #mode: "normal" | "string" | "escape" | "unicode" | "number" | "literal" = "normal";
  #isKey = false;
  #pieces: string[] = [];
  #pending = "";
  #length = 0;
  #hex = "";
  #token = "";
  #literal = "";

  constructor(readonly maxStringCodeUnits = bufferConstants.MAX_STRING_LENGTH) {
    if (!Number.isSafeInteger(maxStringCodeUnits) || maxStringCodeUnits < 0 || maxStringCodeUnits > bufferConstants.MAX_STRING_LENGTH)
      bad("Invalid JSON string capacity");
  }

  write(bytes: Uint8Array): void {
    if (this.#closed || this.#failed || !(bytes instanceof Uint8Array)) bad("Invalid JSON stream state");
    if (bytes.byteLength > 1_048_576) { this.#failed = true; bad("JSON stream chunk exceeds the bounded input size"); }
    try {
      // Bound each intermediate decoded string independently of input chunk size.
      for (let offset = 0; offset < bytes.byteLength; offset += 65_536)
        this.#consume(this.#decoder.decode(bytes.subarray(offset, Math.min(offset + 65_536, bytes.byteLength)), { stream: true }));
    } catch (error) {
      this.#failed = true;
      if (error instanceof TypeError) bad("Expected valid UTF-8 JSON");
      throw error;
    }
  }

  finish(): Json {
    if (this.#closed || this.#failed) bad("Invalid JSON stream state");
    this.#closed = true;
    try {
      this.#consume(this.#decoder.decode());
      if (this.#mode === "number") this.#number();
      if (this.#mode !== "normal" || this.#stack.length || !this.#done) bad("Incomplete JSON stream");
      return this.#root!;
    } catch (error) {
      this.#failed = true;
      if (error instanceof TypeError) bad("Expected valid UTF-8 JSON");
      throw error;
    }
  }

  #append(piece: string): void {
    if (!piece.length) return;
    if (piece.length > this.maxStringCodeUnits - this.#length) bad("JSON string exceeds runtime capacity");
    this.#length += piece.length;
    if (piece.length >= 8192) {
      if (this.#pending) this.#pieces.push(this.#pending);
      this.#pending = ""; this.#pieces.push(piece);
    } else {
      this.#pending += piece;
      if (this.#pending.length >= 8192) { this.#pieces.push(this.#pending); this.#pending = ""; }
    }
  }

  #finishString(): void {
    let value: string;
    try { value = this.#pieces.length ? this.#pieces.join("") + this.#pending : this.#pending; } catch (error) {
      if (error instanceof RangeError) bad("JSON string exceeds runtime capacity");
      throw error;
    }
    this.#pieces = []; this.#pending = ""; this.#length = 0; this.#mode = "normal";
    if (!this.#isKey) { this.#put(value); return; }
    const frame = this.#stack.at(-1);
    if (!frame || frame.kind !== "object" || frame.state !== "first" && frame.state !== "key") bad("Invalid JSON object key");
    if (frame.keys.has(value)) bad("Duplicate JSON object key");
    frame.keys.add(value); frame.key = value; frame.state = "colon";
  }

  #number(): void {
    const token = this.#token;
    const value = Number(token);
    if (!Number.isFinite(value) || Number.isInteger(value) && !Number.isSafeInteger(value)
        || JSON.stringify(value) !== token) bad("Invalid canonical JSON number");
    this.#token = ""; this.#mode = "normal"; this.#put(value);
  }

  #put(value: Json): void {
    const frame = this.#stack.at(-1);
    if (!frame) {
      if (this.#done) bad("Unexpected trailing JSON data");
      this.#root = value; this.#done = true; return;
    }
    if (frame.kind === "array") {
      if (frame.state !== "first" && frame.state !== "value") bad("Expected JSON array separator");
      frame.value.push(value); frame.state = "separator"; return;
    }
    if (frame.state !== "value" || frame.key === undefined) bad("Expected JSON object value");
    Object.defineProperty(frame.value, frame.key, { value, enumerable: true, writable: true, configurable: true });
    frame.key = undefined; frame.state = "separator";
  }

  #startValue(char: string): void {
    if (char === '"') { this.#mode = "string"; this.#isKey = false; return; }
    if (char === "[") { this.#stack.push({ kind: "array", value: [], state: "first" }); return; }
    if (char === "{") { this.#stack.push({ kind: "object", value: {}, keys: new Set(), state: "first" }); return; }
    if (char === "-" || char >= "0" && char <= "9") { this.#mode = "number"; this.#token = char; return; }
    const literal = char === "t" ? "true" : char === "f" ? "false" : char === "n" ? "null" : "";
    if (!literal) bad("Invalid JSON value");
    this.#mode = "literal"; this.#literal = literal; this.#token = char;
  }

  #consume(text: string): void {
    let index = 0;
    while (index < text.length) {
      if (this.#mode === "string") {
        const marker = /["\\\x00-\x1f]/g; marker.lastIndex = index;
        const next = marker.exec(text);
        if (!next) { this.#append(text.slice(index)); return; }
        this.#append(text.slice(index, next.index)); index = next.index + 1;
        if (next[0] === '"') this.#finishString();
        else if (next[0] === "\\") this.#mode = "escape";
        else bad("Invalid JSON string");
        continue;
      }
      const char = text[index]!;
      if (this.#mode === "escape") {
        const escapes: Record<string, string> = { '"': '"', "\\": "\\", "/": "/", b: "\b", f: "\f", n: "\n", r: "\r", t: "\t" };
        if (char === "u") { this.#hex = ""; this.#mode = "unicode"; }
        else if (Object.hasOwn(escapes, char)) { this.#append(escapes[char]!); this.#mode = "string"; }
        else bad("Invalid JSON string escape");
        index++; continue;
      }
      if (this.#mode === "unicode") {
        if (!/^[0-9a-fA-F]$/.test(char)) bad("Invalid JSON unicode escape");
        this.#hex += char; index++;
        if (this.#hex.length === 4) { this.#append(String.fromCharCode(parseInt(this.#hex, 16))); this.#mode = "string"; }
        continue;
      }
      if (this.#mode === "literal") {
        if (char !== this.#literal[this.#token.length]) bad("Invalid JSON literal");
        this.#token += char; index++;
        if (this.#token === this.#literal) {
          const value = this.#literal === "true" ? true : this.#literal === "false" ? false : null;
          this.#token = ""; this.#mode = "normal"; this.#put(value);
        }
        continue;
      }
      if (this.#mode === "number") {
        if (numeric.test(char)) {
          if (this.#token.length >= 32) bad("Invalid canonical JSON number");
          this.#token += char; index++; continue;
        }
        this.#number(); continue;
      }
      if (whitespace(char)) { index++; continue; }
      const frame = this.#stack.at(-1);
      if (!frame) {
        if (this.#done) bad("Unexpected trailing JSON data");
        this.#startValue(char); index++; continue;
      }
      if (frame.kind === "array") {
        if (frame.state === "first" && char === "]" || frame.state === "separator" && char === "]") {
          this.#stack.pop(); this.#put(frame.value); index++; continue;
        }
        if (frame.state === "separator") {
          if (char !== ",") bad("Expected JSON array separator");
          frame.state = "value"; index++; continue;
        }
        this.#startValue(char); index++; continue;
      }
      if ((frame.state === "first" || frame.state === "separator") && char === "}") {
        this.#stack.pop(); this.#put(frame.value); index++; continue;
      }
      if (frame.state === "separator") {
        if (char !== ",") bad("Expected JSON object separator");
        frame.state = "key"; index++; continue;
      }
      if (frame.state === "first" || frame.state === "key") {
        if (char !== '"') bad("Expected JSON object key");
        this.#isKey = true; this.#mode = "string"; index++; continue;
      }
      if (frame.state === "colon") {
        if (char !== ":") bad("Expected JSON colon");
        frame.state = "value"; index++; continue;
      }
      this.#startValue(char); index++;
    }
  }
}
