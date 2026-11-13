import { createHash } from "node:crypto";
import { canonicalJsonChunks, harnessError, validateNodeRequest, type Json, type NodeRPCMap } from "../contract/index.js";
import { ReportTransferBindings, ValueStore, verifyReportValue, type SQLWrite, type ValuePublication, type VerifiedReport, type DiagnosticTransferReceipt } from "../persistence/index.js";
import { readCanonicalStagedValue, reportStagingOwner, type ReportFragment, type ReportFragmentOwner } from "./canonical-report-staging.js";
import { ReportTransferConcurrency } from "./report-transfer-concurrency.js";

type Params = NodeRPCMap["reportAction"]["params"];
type Source = Omit<Params, "report">;
type Control = { request: Extract<ReturnType<typeof validateNodeRequest>, { method: "reportAction" }>; transferID: string; length: number; digest: string };

/** Injected coordinator operations; the worker callback must freshly verify
 * the credential and all installed fixed-definition grants. */
export interface BoundReportTransferHooks {
  prepare(source: Source, principalID: string, assertWorker: () => void): { owner: string; businessAuthorized: boolean; guards: SQLWrite[] };
  preparePublished(source: Source, principalID: string, assertWorker: () => void): { owner: string; businessAuthorized: boolean; guards: SQLWrite[] };
  publish(params: Params, ref: ValuePublication, proof: VerifiedReport, transfer: { id: string; sourceDigest: string; workerDigest: string }, principalID: string, assertWorker: () => void): Promise<{ stored: true } | { stored: false; diagnostic: true }>;
}

const record = (value: unknown): value is Record<string, Json> => !!value && typeof value === "object" && !Array.isArray(value);
const digest = (input: unknown): string => { const hash = createHash("sha256"); for (const bytes of canonicalJsonChunks(input)) hash.update(bytes); return hash.digest("hex"); };

/** Private v4 assembly seam. The caller authenticates before each operation;
 * this class persists source binding before accepting any report content. */
export class BoundReportTransfer {
  readonly #flights = new Map<string, Promise<void>>();
  readonly #diagnosticReplays = new Map<string, { offset: number; hash: ReturnType<typeof createHash>; expires: number }>();
  #waiting = 0;
  constructor(readonly values: ValueStore, readonly bindings: ReportTransferBindings, readonly hooks: BoundReportTransferHooks,
    readonly concurrency = new ReportTransferConcurrency()) {}

  private control(value: Json, owner: ReportFragmentOwner, transferID: string): Control {
    if (!record(value) || Object.keys(value).sort().join(",") !== "digest,length,request,transferID" || value.transferID !== transferID
        || typeof value.length !== "number" || !Number.isSafeInteger(value.length) || value.length <= 0
        || typeof value.digest !== "string" || !/^[a-f0-9]{64}$/.test(value.digest))
      throw harnessError("INVALID_ARGUMENT", "Invalid report transfer control");
    const request = validateNodeRequest(value.request);
    if (request.method !== "reportAction" || request.binding.domainID !== owner.domainID || request.binding.principalID !== owner.principalID
        || digest(request.params.nodeID) !== owner.nodeID || request.params.report.kind !== "report" || request.params.report.value !== null)
      throw harnessError("FORBIDDEN", "Report transfer source is not bound to the authenticated worker");
    return { request, transferID, length: value.length, digest: value.digest };
  }

  private identity(owner: ReportFragmentOwner, transferID: string) {
    const workerDigest = digest([owner.domainID, owner.principalID, owner.nodeID]);
    return { id: digest(["report-transfer-v4", workerDigest, transferID]), workerDigest };
  }

  /** Reauthorize a frozen v4 response without replaying a mutation. */
  async authorizeResponse(owner: ReportFragmentOwner, transferID: string, assertWorker: (nodeID?: string, incarnation?: string) => void): Promise<void> {
    return this.concurrency.run(() => this.authorizeResponseAdmitted(owner, transferID, assertWorker));
  }

  private async authorizeResponseAdmitted(owner: ReportFragmentOwner, transferID: string, assertWorker: (nodeID?: string, incarnation?: string) => void): Promise<void> {
    const transfer = this.identity(owner, transferID), diagnostic = this.bindings.diagnostic(transfer.id, transfer.workerDigest);
    if (diagnostic) { await this.authorizeDiagnostic(owner, transferID, diagnostic, assertWorker); return; }
    const binding = this.bindings.lookup(transfer.id, transfer.workerDigest);
    if (binding.state === "deleting") throw harnessError("FORBIDDEN", "Report transfer is unavailable");
    const ref = this.values.status(binding.controlID, reportStagingOwner(owner));
    if (ref.digest !== binding.sourceDigest) throw harnessError("FORBIDDEN", "Report transfer control changed");
    const control = this.control(await readCanonicalStagedValue(this.values, ref), owner, transferID);
    const { report: _report, ...source } = control.request.params;
    const assertSourceWorker = () => assertWorker(source.nodeID, source.incarnation);
    const prepared = binding.state === "published" ? this.hooks.preparePublished(source, owner.principalID, assertSourceWorker)
      : this.hooks.prepare(source, owner.principalID, assertSourceWorker);
    const report = this.values.status(binding.reportID, prepared.owner);
    if (report.length !== control.length || report.digest !== control.digest) throw harnessError("FORBIDDEN", "Report transfer content changed");
    if (binding.state === "published") this.bindings.verifyPublished(transfer.id, report.id, binding.sourceDigest, transfer.workerDigest, prepared.guards);
    else this.bindings.store.checkGuards([...prepared.guards, this.bindings.chunkGuard(transfer.id, report.id, binding.sourceDigest, transfer.workerDigest)]);
    assertSourceWorker();
  }

  /** Control must already be sealed, canonical-verified and owned by this
   * authenticated worker. Only then may the empty report manifest be bound. */
  async open(owner: ReportFragmentOwner, transferID: string, controlRef: ValuePublication, assertWorker: (nodeID?: string, incarnation?: string) => void): Promise<void> {
    return this.concurrency.run(() => this.openAdmitted(owner, transferID, controlRef, assertWorker));
  }

  private async openAdmitted(owner: ReportFragmentOwner, transferID: string, controlRef: ValuePublication, assertWorker: (nodeID?: string, incarnation?: string) => void): Promise<void> {
    if (controlRef.owner !== reportStagingOwner(owner)) throw harnessError("FORBIDDEN", "Report control owner does not match");
    const value = await readCanonicalStagedValue(this.values, controlRef);
    assertWorker();
    const control = this.control(value, owner, transferID);
    const { report: _report, ...source } = control.request.params;
    const assertSourceWorker = () => assertWorker(source.nodeID, source.incarnation);
    const transfer = this.identity(owner, transferID);
    const diagnostic = this.bindings.diagnostic(transfer.id, transfer.workerDigest);
    if (diagnostic) {
      if (diagnostic.controlID !== controlRef.id || diagnostic.sourceDigest !== controlRef.digest || diagnostic.reportLength !== control.length
          || diagnostic.reportDigest !== control.digest) throw harnessError("REQUEST_CONFLICT", "Diagnostic report transfer identity changed");
      await this.authorizeDiagnostic(owner, transferID, diagnostic, assertWorker);
      return;
    }
    const previous = this.bindings.store.query("SELECT state,control_value_id,report_value_id,source_digest,worker_digest FROM report_transfer_bindings WHERE id=?", [transfer.id])[0];
    const prepared = previous?.state === "published" ? this.hooks.preparePublished(source, owner.principalID, assertSourceWorker)
      : this.hooks.prepare(source, owner.principalID, assertSourceWorker);
    if (previous?.state === "published") {
      if (previous.control_value_id !== controlRef.id || previous.report_value_id !== `report-v4:${transfer.id}`
          || previous.source_digest !== controlRef.digest || previous.worker_digest !== transfer.workerDigest)
        throw harnessError("REQUEST_CONFLICT", "Published report transfer identity changed");
      const report = this.values.status(previous.report_value_id as string, prepared.owner);
      if (report.state !== "published" || report.length !== control.length || report.digest !== control.digest)
        throw harnessError("REQUEST_CONFLICT", "Published report content changed");
      this.bindings.verifyPublished(transfer.id, report.id, controlRef.digest, transfer.workerDigest, prepared.guards);
      return;
    }
    this.bindings.bind({ ...transfer, sourceDigest: controlRef.digest, control: controlRef,
      report: { id: `report-v4:${transfer.id}`, owner: prepared.owner, encoding: "utf8-v1", length: control.length, digest: control.digest } }, prepared.guards);
  }

  /** Each call rehydrates the sealed control and reauthenticates its original
   * source. Finalization alone may atomically publish an authoritative report. */
  async accept(owner: ReportFragmentOwner, meta: ReportFragment, bytes: Uint8Array,
    final: { expiresAt: number; observationOnly: boolean } | undefined, assertWorker: (nodeID?: string, incarnation?: string) => void): Promise<{ complete: false; nextOffset: number } | { complete: true; stored: true } | { complete: true; stored: false; diagnostic: true }> {
    return this.concurrency.run(() => this.acceptAdmitted(owner, meta, bytes, final, assertWorker));
  }

  private async acceptAdmitted(owner: ReportFragmentOwner, meta: ReportFragment, bytes: Uint8Array,
    final: { expiresAt: number; observationOnly: boolean } | undefined, assertWorker: (nodeID?: string, incarnation?: string) => void): Promise<{ complete: false; nextOffset: number } | { complete: true; stored: true } | { complete: true; stored: false; diagnostic: true }> {
    if (!meta || !/^[A-Za-z0-9_-]{1,128}$/.test(meta.transferID) || !Number.isSafeInteger(meta.offset) || meta.offset < 0
        || !Number.isSafeInteger(meta.total) || meta.total <= 0 || !/^[a-f0-9]{64}$/.test(meta.digest)
        || !(bytes instanceof Uint8Array) || !bytes.byteLength || meta.offset >= meta.total || meta.offset % 65_536
        || bytes.length !== Math.min(65_536, meta.total - meta.offset)) throw harnessError("INVALID_ARGUMENT", "Invalid bound report fragment");
    const transfer = this.identity(owner, meta.transferID);
    if (this.#waiting >= 64 || !this.#flights.has(transfer.id) && this.#flights.size >= 64) throw harnessError("LIMIT_EXCEEDED", "Report transfer is busy");
    this.#waiting++;
    try { while (this.#flights.has(transfer.id)) await this.#flights.get(transfer.id); } finally { this.#waiting--; }
    let release!: () => void;
    this.#flights.set(transfer.id, new Promise<void>(resolve => { release = resolve; }));
    try {
      assertWorker();
      const diagnostic = this.bindings.diagnostic(transfer.id, transfer.workerDigest);
      if (diagnostic) return await this.replayDiagnostic(owner, meta, bytes, final, diagnostic, assertWorker);
      const binding = this.bindings.lookup(transfer.id, transfer.workerDigest);
      if (binding.state === "deleting") throw harnessError("STATE_CONFLICT", "Report transfer is being collected");
      const controlRef = this.values.status(binding.controlID, reportStagingOwner(owner));
      if (controlRef.digest !== binding.sourceDigest) throw harnessError("FORBIDDEN", "Report transfer control changed");
      const control = this.control(await readCanonicalStagedValue(this.values, controlRef), owner, meta.transferID);
      assertWorker();
      const { report: _report, ...source } = control.request.params;
      const assertSourceWorker = () => assertWorker(source.nodeID, source.incarnation);
      const prepared = binding.state === "published" ? this.hooks.preparePublished(source, owner.principalID, assertSourceWorker)
        : this.hooks.prepare(source, owner.principalID, assertSourceWorker);
      const ref = this.values.status(binding.reportID, prepared.owner);
      if (ref.length !== control.length || ref.digest !== control.digest || ref.length !== meta.total || ref.digest !== meta.digest)
        throw harnessError("REQUEST_CONFLICT", "Report transfer content changed");
      if (binding.state === "active") {
        this.values.append(ref.id, ref.owner, meta.offset, bytes,
          [...prepared.guards, this.bindings.chunkGuard(transfer.id, ref.id, binding.sourceDigest, transfer.workerDigest)]);
      } else {
        this.bindings.verifyPublished(transfer.id, ref.id, binding.sourceDigest, transfer.workerDigest, prepared.guards);
        const part = this.values.readChunk(ref.id, ref.owner, meta.offset, bytes.length);
        if (part.bytes.length !== bytes.length || !Buffer.from(part.bytes).equals(bytes)) throw harnessError("REQUEST_CONFLICT", "Published report fragment differs");
      }
      if (meta.offset + bytes.length < meta.total) return { complete: false, nextOffset: meta.offset + bytes.length };
      if (!final || !Number.isSafeInteger(final.expiresAt) || final.expiresAt <= 0 || typeof final.observationOnly !== "boolean"
          || source.observationOnly === true && !final.observationOnly) throw harnessError("INVALID_ARGUMENT", "Invalid report finalization");
      const publication = await this.values.seal(ref.id, ref.owner);
      const proof = await verifyReportValue(this.bindings.store, publication);
      assertSourceWorker();
      const request = validateNodeRequest({ ...control.request, params: { ...source, grant: { ...source.grant, expiresAt: final.expiresAt },
        observationOnly: final.observationOnly, report: { kind: "report", value: null } } });
      if (request.method !== "reportAction") throw harnessError("INVALID_ARGUMENT", "Invalid report finalization");
      const outcome = await this.hooks.publish(request.params, publication, proof, { ...transfer, sourceDigest: binding.sourceDigest }, owner.principalID, assertSourceWorker);
      return outcome.stored ? { complete: true, stored: true } : { complete: true, stored: false, diagnostic: true };
    } finally { this.#flights.delete(transfer.id); release(); }
  }

  private async authorizeDiagnostic(owner: ReportFragmentOwner, transferID: string, receipt: DiagnosticTransferReceipt,
    assertWorker: (nodeID?: string, incarnation?: string) => void): Promise<Control> {
    const ref = this.values.status(receipt.controlID, reportStagingOwner(owner));
    if (ref.state !== "published" || ref.digest !== receipt.sourceDigest) throw harnessError("FORBIDDEN", "Diagnostic transfer control changed");
    const control = this.control(await readCanonicalStagedValue(this.values, ref), owner, transferID);
    if (control.length !== receipt.reportLength || control.digest !== receipt.reportDigest) throw harnessError("FORBIDDEN", "Diagnostic transfer content changed");
    const { report: _report, ...source } = control.request.params;
    if (source.actionID !== receipt.actionID || source.attemptID !== receipt.attemptID || source.phase !== receipt.phase)
      throw harnessError("FORBIDDEN", "Diagnostic transfer source changed");
    const check = () => assertWorker(source.nodeID, source.incarnation);
    const prepared = this.hooks.preparePublished(source, owner.principalID, check);
    if (prepared.owner !== receipt.reportOwner) throw harnessError("FORBIDDEN", "Diagnostic transfer owner changed");
    this.bindings.store.checkGuards(prepared.guards); check();
    return control;
  }

  private async replayDiagnostic(owner: ReportFragmentOwner, meta: ReportFragment, bytes: Uint8Array,
    final: { expiresAt: number; observationOnly: boolean } | undefined, receipt: DiagnosticTransferReceipt,
    assertWorker: (nodeID?: string, incarnation?: string) => void): Promise<{ complete: false; nextOffset: number } | { complete: true; stored: false; diagnostic: true }> {
    const control = await this.authorizeDiagnostic(owner, meta.transferID, receipt, assertWorker);
    if (meta.total !== receipt.reportLength || meta.digest !== receipt.reportDigest) throw harnessError("REQUEST_CONFLICT", "Diagnostic report content changed");
    if (meta.offset + bytes.length === meta.total && (!final || !Number.isSafeInteger(final.expiresAt) || final.expiresAt <= 0
        || typeof final.observationOnly !== "boolean" || control.request.params.observationOnly === true && !final.observationOnly))
      throw harnessError("INVALID_ARGUMENT", "Invalid report finalization");
    const now = Date.now();
    for (const [id, state] of this.#diagnosticReplays) if (state.expires <= now) this.#diagnosticReplays.delete(id);
    let state = this.#diagnosticReplays.get(receipt.id);
    if (meta.offset === 0) {
      if (!state && this.#diagnosticReplays.size >= 64) throw harnessError("LIMIT_EXCEEDED", "Diagnostic replay is busy");
      state = { offset: 0, hash: createHash("sha256"), expires: now + 120_000 }; this.#diagnosticReplays.set(receipt.id, state);
    }
    if (!state || state.offset !== meta.offset) throw harnessError("INVALID_ARGUMENT", "Diagnostic replay must begin at offset zero and remain contiguous");
    state.hash.update(bytes); state.offset += bytes.length; state.expires = now + 120_000;
    if (state.offset < meta.total) return { complete: false, nextOffset: state.offset };
    this.#diagnosticReplays.delete(receipt.id);
    if (state.hash.digest("hex") !== receipt.reportDigest) throw harnessError("REQUEST_CONFLICT", "Diagnostic replay bytes differ from the original report");
    await this.bindings.finishDiagnosticRetirement(receipt);
    await this.authorizeDiagnostic(owner, meta.transferID, receipt, assertWorker);
    return { complete: true, stored: false, diagnostic: true };
  }
}
