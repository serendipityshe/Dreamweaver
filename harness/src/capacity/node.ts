import { expandDispatchPayload } from "../contract/index.js";
import { nodeStopSnapshot, canonicalJsonDigest, dispatchOriginalMatches } from "../contract/index.js";
import { createHash, randomUUID } from "node:crypto";
import { LeaseClock } from "./lease-clock.js";
import type { SpoolSource } from "./report-spool.js";
import { AsyncReportSpool } from "./report-spool-async.js";
import { InternalCanonicalJsonDecoder } from "../contract/index.js";
import {
  assertJsonValueSize, canonicalJson, copyJson, isCopyJsonValidationError, harnessError, HarnessError, toHarnessError, validateActionRequest, validatePreparedActionRequest, validateDecision, validateReport, validateRPCRequest,
  type AgentDriver, type AppliedPolicy, type CapacityDefinition, type DependencyBinding,
  type DefinitionDescriptor, type HarnessNode, type HostPolicy, type NodeCall, type NodeOptions,
  type NodeSession, type NodeStopResult, type Ref, type SessionBinding, type ValidationAssignment,
  type ValidationConclusion, type DriverAssignment, type DriverConclusion, type FinalizeAssignment, type FinalizeConclusion, type StartAssignment, type WakeAssignment, type BusinessLeaseGrant,
  type ActionAssignment, type ActionReport, type CapacityContext, type CapacityResult, type Checkpoint, type Json, type NodeRPCMap, type CallOptions,
} from "../contract/index.js";

interface NodeEnvironment { binding: SessionBinding; call: NodeCall; maxValueBytes: number; maxResponseBytes?: number; isClosed?(): boolean;
  confirmCheckpoint?(params: NodeRPCMap["readCheckpoint"]["params"], expected: Checkpoint, options?: CallOptions): Promise<NodeRPCMap["readCheckpoint"]["result"]>;
  sendStagedReport?(source: Omit<NodeRPCMap["reportAction"]["params"], "report" | "observationOnly">,
    chunks: { key: string; length: number; digest: string; read(offset: number): Promise<Uint8Array> },
    current: () => { grant: BusinessLeaseGrant; observationOnly: boolean }, signal?: AbortSignal): Promise<{ stored: true } | { stored: false; diagnostic: true }> }
type Definition = AgentDriver | CapacityDefinition<unknown>;
const identifier = (value: unknown): value is string => typeof value === "string" && value.length > 0 && value.trim() === value;
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const fields = (value: object, names: string[]) => Object.keys(value).every(name => names.includes(name));
const key = (kind: string, ref: Ref) => JSON.stringify([kind, ref.id, ref.version]);
const stopped = (): NodeStopResult => ({ stopped: true, remainingRunIDs: [], remainingActionIDs: [] });

/** Local code assembly. Only descriptors and validation data cross the wire. */
export function createHarnessNode(options: NodeOptions, environment: NodeEnvironment): HarnessNode {
  return new LocalNode(options, environment);
}

class LocalNode implements HarnessNode {
  readonly #options: NodeOptions;
  readonly #definitions = new Map<string, Definition>();
  readonly #bindings = new Map<string, DependencyBinding<unknown>>();
  readonly #policies = new Map<string, HostPolicy>();
  readonly #incarnation = randomUUID();
  readonly #abort = new AbortController();
  readonly #wakeups = new Set<() => void>();
  readonly #callbacks = new Set<Promise<void>>();
  readonly #leases = new Set<LeaseClock>();
  #phase: "new" | "starting" | "started" | "draining" | "interrupting" | "stopped" = "new";
  #claimAbort: AbortController | undefined;
  #pendingClaimRequestID: string | undefined;
  #unconfirmedStoppedClaim = false;
  readonly #activeRuns = new Set<string>();
  readonly #activeActions = new Set<string>();
  readonly #activeGrants = new Set<() => BusinessLeaseGrant>();
  readonly #activeValidations = new Set<{ runID?: string; actionID?: string }>();
  #unregistering: Promise<void> | undefined;
  #interruptCleanup: Promise<void> | undefined;
  #quiescing: Promise<void> | undefined;
  #quiesceParams: { nodeID: string; incarnation: string; pendingClaimRequestID?: string } | undefined;
  #quiesced = false;
  #session: NodeSession | undefined;
  #attempted = false;
  #worker: Promise<void> | undefined;
  #heartbeat: Promise<void> | undefined;
  #starting: Promise<void> | undefined;
  #unregistered = false;
  #invalidAsyncValidator = false;
  #preparationActive = false;
  #spool: AsyncReportSpool | undefined;
  #spoolClosing: Promise<void> | undefined;
  #spoolCloseFailed = false;
  #recoveringSpool = false;
  readonly #spoolOutstanding = new Set<string>();

  constructor(options: NodeOptions, readonly environment: NodeEnvironment) {
    if (!record(options) || !fields(options, ["nodeID", "concurrency", "policies", "reportSpool", "onProgress", "onMetric", "onError"])
        || !identifier(options.nodeID) || !record(options.concurrency) || !fields(options.concurrency, ["runs", "actions"])
        || !positive(options.concurrency.runs) || !positive(options.concurrency.actions)
        || [options.onProgress, options.onMetric, options.onError].some(callback => callback !== undefined && typeof callback !== "function")) {
      throw harnessError("INVALID_ARGUMENT", "Invalid node options");
    }
    const policies = options.policies ?? [];
    if (options.reportSpool !== undefined && (!record(options.reportSpool) || !fields(options.reportSpool, ["directory", "maxBytes"])
        || !identifier(options.reportSpool.directory) || !positive(options.reportSpool.maxBytes))) throw harnessError("INVALID_ARGUMENT", "Invalid report spool configuration");
    if (!Array.isArray(policies)) throw harnessError("INVALID_ARGUMENT", "Invalid selected policies");
    const copied: AppliedPolicy[] = policies.map(policy => {
      if (!record(policy) || !fields(policy, ["ref", "config"]) || !Object.hasOwn(policy, "config")) throw harnessError("INVALID_ARGUMENT", "Invalid selected policy");
      const ref = checkedRef(policy.ref), config = copyJson(policy.config);
      assertJsonValueSize(config, environment.maxValueBytes);
      return { ref, config };
    });
    if (new Set(copied.map(policy => key("policy", policy.ref))).size !== copied.length) throw harnessError("DUPLICATE_DEFINITION", "A selected policy is duplicated");
    this.#options = { ...options, concurrency: { ...options.concurrency }, policies: copied,
      ...(options.reportSpool ? { reportSpool: { ...options.reportSpool } } : {}) };
  }

  registerAgent(driver: AgentDriver): void {
    this.#beforeStart();
    if (!record(driver) || !fields(driver, ["ref", "validateInput", "validateOutput", "advance"])
        || typeof driver.validateInput !== "function" || typeof driver.validateOutput !== "function" || typeof driver.advance !== "function") {
      throw harnessError("INVALID_ARGUMENT", "An Agent requires input/output validators and advance");
    }
    const ref = checkedRef(driver.ref), identity = key("agent", ref);
    this.#unique(identity);
    this.#definitions.set(identity, { ref, validateInput: driver.validateInput.bind(driver),
      validateOutput: driver.validateOutput.bind(driver), advance: driver.advance.bind(driver) });
  }

  registerCapacity<TDeps>(definition: CapacityDefinition<TDeps>, binding: DependencyBinding<TDeps>): void {
    this.#beforeStart();
    if (!record(definition) || !fields(definition, ["ref", "validateInput", "validateOutput", "execute", "reconcile"])
        || typeof definition.validateInput !== "function" || typeof definition.validateOutput !== "function" || typeof definition.execute !== "function"
        || definition.reconcile !== undefined && typeof definition.reconcile !== "function"
        || !record(binding) || !fields(binding, ["create", "dispose"]) || typeof binding.create !== "function"
        || binding.dispose !== undefined && typeof binding.dispose !== "function") {
      throw harnessError("INVALID_ARGUMENT", "A Capacity requires validators, execute and a dependency binding");
    }
    const ref = checkedRef(definition.ref), identity = key("capacity", ref);
    this.#unique(identity);
    this.#definitions.set(identity, { ref, validateInput: definition.validateInput.bind(definition), validateOutput: definition.validateOutput.bind(definition),
      execute: definition.execute.bind(definition), ...(definition.reconcile ? { reconcile: definition.reconcile.bind(definition) } : {}) } as CapacityDefinition<unknown>);
    this.#bindings.set(identity, { create: binding.create.bind(binding), ...(binding.dispose ? { dispose: binding.dispose.bind(binding) } : {}) } as DependencyBinding<unknown>);
  }

  registerPolicy(policy: HostPolicy): void {
    this.#beforeStart();
    if (!record(policy) || !fields(policy, ["ref", "rewrite", "authorize"])
        || policy.rewrite !== undefined && typeof policy.rewrite !== "function" || policy.authorize !== undefined && typeof policy.authorize !== "function") {
      throw harnessError("INVALID_ARGUMENT", "Invalid policy definition");
    }
    const ref = checkedRef(policy.ref), identity = key("policy", ref);
    this.#unique(identity);
    this.#policies.set(identity, { ref, ...(policy.rewrite ? { rewrite: policy.rewrite.bind(policy) } : {}),
      ...(policy.authorize ? { authorize: policy.authorize.bind(policy) } : {}) });
  }

  start(): Promise<void> {
    this.#beforeStart();
    if (this.#definitions.size === 0) return Promise.reject(harnessError("NODE_STATE_INVALID", "A node requires an Agent or Capacity definition"));
    for (const policy of this.#options.policies!) {
      if (!this.#policies.has(key("policy", policy.ref))) return Promise.reject(harnessError("VERSION_UNAVAILABLE", "A selected policy is not registered on this node"));
    }
    this.#phase = "starting";
    const definitions: DefinitionDescriptor[] = [
      ...[...this.#definitions].map(([identity, definition]) => {
        const kind = JSON.parse(identity)[0] as "agent" | "capacity";
        return { kind, ref: definition.ref, ...(kind === "capacity" ? { supportsReconcile: typeof (definition as CapacityDefinition<unknown>).reconcile === "function" } : {}) };
      }),
      ...[...this.#policies.values()].map(policy => ({ kind: "policy" as const, ref: policy.ref })),
    ];
    this.#attempted = true;
    return this.#starting = (async () => {
      try {
        if (this.#options.reportSpool) {
          this.#spool = new AsyncReportSpool(this.#options.reportSpool.directory, this.#options.reportSpool.maxBytes, this.environment.binding, this.#options.nodeID);
          await this.#spool.validate(this.environment.binding, this.#options.nodeID);
          for (const actionID of await this.#spool.actionIDs()) this.#spoolOutstanding.add(actionID);
          if (this.#phase !== "starting") throw harnessError("NODE_STATE_INVALID", "Node stopped during spool validation");
        }
        this.#session = await this.#retry(() => this.environment.call("register", { ...this.#identity(), definitions,
          concurrency: this.#options.concurrency, policies: this.#options.policies! }, this.#callOptions()));
        if (this.#phase !== "starting") throw harnessError("NODE_STATE_INVALID", "Node stopped during startup");
        this.#phase = "started";
        if (this.#spool) await this.#startSpoolRecovery();
        if (this.#phase !== "started") throw harnessError("NODE_STATE_INVALID", "Node stopped during spool recovery startup");
        this.#worker = this.#work();
        this.#heartbeat = this.#maintainSession();
      } catch (error) {
        if (this.#spool) { try { await this.#spool.close(); } catch {} this.#spool = undefined; }
        this.#phase = "stopped";
        this.#abort.abort();
        this.#wake();
        throw error;
      }
    })();
  }

  async stop(options: { mode: "drain" | "interrupt"; timeoutMs: number }): Promise<NodeStopResult> {
    if (!record(options) || !fields(options, ["mode", "timeoutMs"]) || !["drain", "interrupt"].includes(options.mode)
        || !Number.isSafeInteger(options.timeoutMs) || options.timeoutMs < 0) throw harnessError("INVALID_ARGUMENT", "Invalid node stop options");
    const deadline = performance.now() + options.timeoutMs;
    if (this.#phase === "interrupting" && options.mode === "drain") throw harnessError("NODE_STATE_INVALID", "An interrupted node cannot return to drain");
    if (this.#phase !== "stopped") {
      const interrupt = options.mode === "interrupt" || this.#phase === "new" || this.#phase === "starting";
      if (interrupt) {
        this.#interrupt();
      } else {
        this.#phase = "draining";
        this.#quiesce();
      }
      this.#claimAbort?.abort();
      this.#wake();
    }
    const completed = await within(Promise.allSettled([this.#starting, this.#worker, this.#quiescing, ...this.#callbacks]), remaining(deadline));
    if (completed && this.#callbacks.size === 0 && !this.#unconfirmedStoppedClaim && this.#spoolOutstanding.size === 0) {
      this.#phase = "stopped";
      this.#abort.abort();
      this.#wake();
      await within(this.#interruptCleanup ?? this.#unregister(), remaining(deadline));
      this.#definitions.clear(); this.#bindings.clear(); this.#policies.clear();
      if (this.#spool) { const spool = this.#spool; this.#spool = undefined; this.#closeSpool(spool); }
    } else if (this.#phase === "interrupting") {
      if (this.#interruptCleanup) await within(this.#interruptCleanup, remaining(deadline));
    }
    const spoolClosed = !this.#spoolClosing || await within(this.#spoolClosing, remaining(deadline));
    const result = this[nodeStopSnapshot]();
    result.stopped = completed && spoolClosed && !this.#spoolCloseFailed && this.#callbacks.size === 0 && !this.#unconfirmedStoppedClaim && this.#spoolOutstanding.size === 0;
    return result;
  }

  [nodeStopSnapshot](): NodeStopResult {
    return { stopped: this.#callbacks.size === 0 && this.#phase === "stopped" && !this.#spoolCloseFailed && this.#spoolOutstanding.size === 0,
      remainingRunIDs: [...new Set([...this.#activeRuns, ...[...this.#activeValidations].flatMap(value => value.runID ? [value.runID] : [])])],
      remainingActionIDs: [...new Set([...this.#activeActions, ...this.#spoolOutstanding, ...[...this.#activeValidations].flatMap(value => value.actionID ? [value.actionID] : [])])] };
  }

  async #unregister(): Promise<void> {
    if (!this.#attempted || this.#unregistered) return;
    if (!this.#unregistering) this.#unregistering = (async () => {
      try { await this.environment.call("unregister", this.#identity(), { timeoutMs: 5_000 }); this.#unregistered = true; }
      catch (error) { this.#notify(error); }
    })().finally(() => { this.#unregistering = undefined; });
    await this.#unregistering;
  }
  async #release(grant: BusinessLeaseGrant): Promise<void> {
    try { await this.environment.call("releaseLease", { ...this.#identity(), grant, releaseRequestID: randomUUID() }, { timeoutMs: 5_000 }); }
    catch (error) { this.#notify(error); }
  }

  #identity() { return { nodeID: this.#options.nodeID, incarnation: this.#incarnation }; }
  #callOptions() { return { signal: this.#abort.signal, timeoutMs: 5_000 }; }
  #beforeStart(): void { if (this.#phase !== "new") throw harnessError("NODE_STATE_INVALID", "Definitions can only be registered before starting a node"); }
  #unique(identity: string): void {
    if (this.#definitions.has(identity) || this.#policies.has(identity)) throw harnessError("DUPLICATE_DEFINITION", "The definition type and Ref are already registered");
  }
  #notify(error: unknown): void {
    try { this.#options.onError?.(toHarnessError(error, false).info); } catch { /* Observation callbacks do not control work. */ }
  }
  #closeSpoolIfIdle(): void {
    if (this.#isStopped() && this.#callbacks.size === 0 && this.#spool) {
      const spool = this.#spool; this.#spool = undefined;
      this.#closeSpool(spool);
    }
  }
  #closeSpool(spool: AsyncReportSpool): void {
    this.#spoolClosing = spool.close().catch(error => { this.#spoolCloseFailed = true; this.#notify(error); });
  }
  async #spoolDelay(): Promise<void> {
    await new Promise<void>(resolve => { const timer = setTimeout(resolve, Math.max(1000, this.#session?.pollMs ?? 1000)); timer.unref(); });
  }
  async #startSpoolRecovery(): Promise<void> {
    const spool = this.#spool!, first = this.environment.sendStagedReport ? await spool.head() : await spool.next();
    if (!first) return;
    this.#recoveringSpool = true;
    let entryActionID = first.source.actionID;
    const callback = (async () => {
      let entry = first;
      for (;;) {
        const source = entry.source;
        entryActionID = source.actionID;
        if (typeof source?.actionID === "string") this.#activeActions.add(source.actionID);
        if (source.nodeID !== this.#options.nodeID || source.grant.nodeID !== source.nodeID || source.grant.incarnation !== source.incarnation
            || source.grant.workerPrincipalID !== this.environment.binding.principalID || source.grant.subjectID !== source.actionID
            || source.grant.attemptID !== source.attemptID || source.assignmentID !== source.attemptID) {
          this.#notify(harnessError("SESSION_MISMATCH", "Retained report source does not match this spool"));
          await new Promise<void>(() => {});
        }
        let lastError = "";
        for (;;) {
          try {
            if (this.environment.isClosed?.()) return;
            const { runID: _runID, ...params } = source;
            if ("chunked" in entry && this.environment.sendStagedReport) {
              await this.environment.sendStagedReport(params, await spool.chunks(entry.key), () => ({ grant: source.grant, observationOnly: true }));
            } else {
              const inline = "chunked" in entry ? await spool.next() : entry;
              if (!inline || inline.key !== entry.key) throw harnessError("STATE_CONFLICT", "Retained report changed during recovery");
              await this.environment.call("reportAction", { ...params, report: inline.report, observationOnly: true }, { timeoutMs: 5_000 });
            }
            await spool.remove(entry.key);
            if (!(await spool.actionIDs()).includes(source.actionID)) this.#spoolOutstanding.delete(source.actionID);
            break;
          } catch (error) {
            const info = toHarnessError(error, false).info, identity = JSON.stringify([info.code, info.disposition, info.message]);
            if (identity !== lastError) { lastError = identity; this.#notify(error); }
            await this.#spoolDelay();
          }
        }
        this.#activeActions.delete(source.actionID);
        const next = this.environment.sendStagedReport ? await spool.head() : await spool.next();
        if (!next) return;
        entry = next;
      }
    })().catch(async error => { this.#notify(error); await new Promise<void>(() => {}); })
      .finally(() => { this.#recoveringSpool = false; this.#activeActions.delete(entryActionID); this.#callbacks.delete(callback); this.#closeSpoolIfIdle(); this.#wake(); });
    this.#callbacks.add(callback);
  }
  #wake(): void { for (const wake of this.#wakeups) wake(); }
  #pause(ms: number): Promise<void> {
    if (this.#phase === "interrupting" || this.#phase === "stopped") return Promise.resolve();
    return new Promise(resolve => {
      const done = () => { clearTimeout(timer); this.#wakeups.delete(done); resolve(); };
      const timer = setTimeout(done, Math.min(ms, 2_147_483_647));
      this.#wakeups.add(done);
    });
  }
  async #retry<T>(action: () => Promise<T>, cancelled?: () => boolean): Promise<T> {
    for (;;) {
      try { return await action(); }
      catch (error) {
        if (this.#isStopped() || cancelled?.() || !(error instanceof HarnessError) || error.info.disposition !== "uncertain") throw error;
        for (const lease of this.#leases) lease.disconnect();
        this.#notify(error);
        await this.#pause(this.#session?.pollMs ?? 50);
      }
    }
  }
  async #maintainSession(): Promise<void> {
    while (this.#phase === "started" || this.#phase === "draining") {
      await this.#pause(this.#session!.heartbeatMs);
      if (this.#phase !== "started" && this.#phase !== "draining") return;
      const renewRequestID = randomUUID();
      try { this.#session = await this.#retry(() => this.environment.call("renew", { ...this.#identity(), renewRequestID }, this.#callOptions())); }
      catch (error) { if (!this.#isStopped()) this.#halt(error); return; }
    }
  }
  #isStopped(): boolean { return this.#phase === "interrupting" || this.#phase === "stopped"; }
  #quiesce(): void {
    if (!this.#session || this.#quiescing || this.#quiesced) return;
    const params = this.#quiesceParams ??= { ...this.#identity(), ...(this.#pendingClaimRequestID ? { pendingClaimRequestID: this.#pendingClaimRequestID } : {}) };
    this.#unconfirmedStoppedClaim = params.pendingClaimRequestID !== undefined;
    // Do not attach the local execution abort signal: interrupt must still be
    // able to fence a claim whose response was lost.
    const attempt = this.#retry(() => this.environment.call("quiesce", params, { timeoutMs: 5_000 }))
      .then(() => { this.#quiesced = true; this.#unconfirmedStoppedClaim = false; }).catch(error => { this.#notify(error); })
      .finally(() => { if (this.#quiescing === attempt) this.#quiescing = undefined; });
    this.#quiescing = attempt;
  }
  #interrupt(): void {
    this.#phase = "interrupting";
    this.#quiesce();
    this.#abort.abort();
    for (const lifetime of this.#leases) lifetime.abort();
    if (!this.#interruptCleanup) {
      const cleanup = (async () => {
      await Promise.allSettled([...this.#activeGrants].map(grant => this.#release(grant())));
      await this.#quiescing;
      // An unconfirmed claim must remain associated with a live session so a
      // later stop can retry its exact quiesce receipt.
      if (!this.#unconfirmedStoppedClaim) await this.#unregister();
      })().finally(() => { if (this.#interruptCleanup === cleanup && this.#unconfirmedStoppedClaim) this.#interruptCleanup = undefined; });
      this.#interruptCleanup = cleanup;
    }
    this.#claimAbort?.abort();
    this.#wake();
  }
  #halt(error: unknown): void {
    this.#notify(error);
    this.#interrupt();
  }
  #trackValidation(assignment: ValidationAssignment): () => void {
    const context = assignment.payload.context;
    const subject = record(context) ? { ...(identifier(context.runID) ? { runID: context.runID } : {}),
      ...(identifier(context.actionID) ? { actionID: context.actionID } : {}) } : {};
    this.#activeValidations.add(subject);
    return () => { this.#activeValidations.delete(subject); };
  }
  #trackAsync(value: PromiseLike<unknown>, subject: { runID?: string; actionID?: string }): void {
    this.#activeValidations.add(subject);
    const callback = Promise.resolve(value).then(() => undefined, () => undefined).finally(() => {
      this.#callbacks.delete(callback); this.#activeValidations.delete(subject); this.#closeSpoolIfIdle(); this.#wake();
    });
    this.#callbacks.add(callback);
  }
  #trackAsyncValidation(value: PromiseLike<unknown>, assignment: ValidationAssignment): void {
    const context = assignment.payload.context;
    this.#trackAsync(value, record(context) ? { ...(identifier(context.runID) ? { runID: context.runID } : {}),
      ...(identifier(context.actionID) ? { actionID: context.actionID } : {}) } : {});
  }
  async #work(): Promise<void> {
    let idleDelay = 0;
    while (this.#phase === "started") {
      const claimRequestID = randomUUID();
      const sentAt = performance.now();
      try {
        // A lost reply replays the same logical claim. Active callbacks can
        // finish during retry; their new availability belongs to the next key.
        const claim = { ...this.#identity(), claimRequestID, validationAvailable: !this.#preparationActive,
          runAvailable: this.#activeRuns.size < this.#options.concurrency.runs,
          actionAvailable: !this.#recoveringSpool && this.#activeActions.size < this.#options.concurrency.actions };
        const claimAbort = this.#claimAbort = new AbortController();
        this.#pendingClaimRequestID = claimRequestID;
        const { assignment } = await this.#retry(() => this.environment.call("claim", claim,
          { signal: AbortSignal.any([this.#abort.signal, claimAbort.signal]), timeoutMs: 5_000 }), () => claimAbort.signal.aborted);
        if (this.#claimAbort === claimAbort) this.#claimAbort = undefined;
        if (this.#pendingClaimRequestID === claimRequestID) this.#pendingClaimRequestID = undefined;
        if (this.#phase !== "started") {
          if (assignment && "lease" in assignment) void this.#release(assignment.lease);
          return;
        }
        if (assignment) {
          idleDelay = 0;
          if ("action" in assignment) {
            this.#activeActions.add(assignment.actionID);
            const callback = this.#runAction(assignment, sentAt).catch(error => this.#notify(error)).finally(() => { this.#activeActions.delete(assignment.actionID); this.#callbacks.delete(callback); this.#closeSpoolIfIdle(); this.#wake(); });
            this.#callbacks.add(callback);
            continue;
          }
          if ("driver" in assignment) {
            this.#activeRuns.add(assignment.runID);
            const callback = this.#drive(assignment, sentAt).catch(error => this.#notify(error)).finally(() => { this.#activeRuns.delete(assignment.runID); this.#callbacks.delete(callback); this.#closeSpoolIfIdle(); this.#wake(); });
            this.#callbacks.add(callback);
            continue;
          }
          if ("start" in assignment || "wake" in assignment || "finalize" in assignment) {
            this.#activeRuns.add(assignment.runID);
            try { await this.#acknowledge(assignment, sentAt); }
            finally { this.#activeRuns.delete(assignment.runID); this.#wake(); }
            continue;
          }
          if (assignment.payload.phase === "admission" || assignment.payload.phase === "report-output") {
            this.#preparationActive = true;
            const callback = this.#finishPreparation(assignment).finally(() => { this.#preparationActive = false; this.#callbacks.delete(callback); this.#closeSpoolIfIdle(); this.#wake(); });
            this.#callbacks.add(callback);
            continue;
          }
          const untrack = this.#trackValidation(assignment);
          try {
            const conclusion = await this.#validate(assignment);
            if (this.#isStopped()) return;
            await this.#retry(() => this.environment.call("report", { ...this.#identity(), assignment, conclusion }, this.#callOptions()));
            if (this.#invalidAsyncValidator) { this.#halt(harnessError("INVALID_ARGUMENT", "Validators must return synchronously")); return; }
          } finally { untrack(); }
        } else {
          const base = this.#session!.pollMs;
          const cap = Math.max(base, Math.min(1_000, base * 16));
          await this.#pause(idleDelay || base);
          idleDelay = Math.min(cap, (idleDelay || base) * 2);
        }
      } catch (error) {
        if (this.#phase !== "started") {
          if (this.#pendingClaimRequestID === claimRequestID) this.#pendingClaimRequestID = undefined;
          return;
        }
        if (error instanceof HarnessError && error.info.code === "EXECUTION_FENCED") {
          if (this.#pendingClaimRequestID === claimRequestID) this.#pendingClaimRequestID = undefined;
          // An assignment can expire independently of a still-live node session.
          const renewRequestID = randomUUID();
          try { this.#session = await this.#retry(() => this.environment.call("renew", { ...this.#identity(), renewRequestID }, this.#callOptions())); }
          catch (renewError) { this.#halt(renewError); return; }
          this.#notify(error);
        } else { this.#halt(error); if (this.#pendingClaimRequestID === claimRequestID) this.#pendingClaimRequestID = undefined; return; }
      }
    }
  }
  async #finishPreparation(assignment: ValidationAssignment): Promise<void> {
    const untrack = this.#trackValidation(assignment);
    try {
      const conclusion = await this.#validate(assignment);
      if (!this.#isStopped()) await this.#retry(() => this.environment.call("report", { ...this.#identity(), assignment, conclusion }, this.#callOptions()));
    } catch (error) {
      if (this.#isStopped()) return;
      if (error instanceof HarnessError && error.info.code === "EXECUTION_FENCED") this.#notify(error);
      else this.#halt(error);
    } finally { untrack(); }
  }
  async #runAction(assignment: ActionAssignment, sentAt: number): Promise<void> {
    const reportValueLimit = assignment.reportValueLimit ?? this.environment.maxValueBytes;
    const margin = Math.min(Math.floor(assignment.leaseTTLms / 10), assignment.leaseTTLms - 1);
    const lifetime = new LeaseClock(sentAt, assignment.leaseTTLms, margin);
    this.#leases.add(lifetime);
    let grant = assignment.lease, finished = false, managedBlocked = false, checkpointBusy = false;
    let spoolKey: string | undefined, spoolStored = false;
    const activeGrant = () => grant;
    this.#activeGrants.add(activeGrant);
    let phase: "execute" | "reconcile" = assignment.stage;
    let checkpoint = copyJson(assignment.checkpoint) as unknown as Checkpoint;
    let wake: (() => void) | undefined;
    const signal = AbortSignal.any([this.#abort.signal, lifetime.signal]);
    const pause = (ms: number) => new Promise<void>(resolve => {
      const done = () => { clearTimeout(timer); signal.removeEventListener("abort", done); wake = undefined; resolve(); };
      const timer = setTimeout(done, Math.max(1, ms)); wake = done;
      signal.addEventListener("abort", done, { once: true }); if (signal.aborted) done();
    });
    const renew = (async () => {
      while (!finished && !signal.aborted) {
        await pause(Math.max(1, Math.floor(assignment.leaseTTLms / 3)));
        if (finished || signal.aborted) break;
        const renewRequestID = randomUUID(), renewalSentAt = performance.now();
        for (;;) {
          try {
            const result = await this.environment.call("renewLease", { ...this.#identity(), grant, renewRequestID }, { signal, timeoutMs: 5_000 });
            if (!lifetime.confirmRenewal(renewalSentAt, result.ttlMs)) return;
            grant = result.grant;
            if (!await lifetime.reconnect(async () => true)) return;
            break;
          } catch (error) {
            if (signal.aborted || finished) return;
            if (!(error instanceof HarnessError) || error.info.disposition !== "uncertain") { lifetime.abort(); this.#notify(error); return; }
            lifetime.disconnect(); this.#notify(error); await pause(this.#session!.pollMs);
          }
        }
      }
    })();
    const verify = async (recoveringCheckpoint = false) => {
      if ((managedBlocked && !recoveringCheckpoint) || signal.aborted) throw harnessError("EXECUTION_FENCED", "Action Context is not eligible");
      try {
        if (!lifetime.canOperate()) {
          const reconnected = await lifetime.reconnect(async () => (await this.environment.call("verifyLease", { ...this.#identity(), grant }, { signal, timeoutMs: 5_000 })).valid);
          if (!reconnected) throw harnessError("EXECUTION_FENCED", "Action Context cannot reconnect");
        } else await this.environment.call("verifyLease", { ...this.#identity(), grant }, { signal, timeoutMs: 5_000 });
      } catch (error) {
        if (error instanceof HarnessError && error.info.disposition === "uncertain") lifetime.disconnect();
        else lifetime.abort();
        throw error;
      }
      if ((managedBlocked && !recoveringCheckpoint) || !lifetime.canOperate() || signal.aborted) throw harnessError("EXECUTION_FENCED", "Action Context is not eligible");
    };
    const diagnostic = async (code: string) => {
      try { await this.environment.call("diagnoseAction", { ...this.#identity(), grant, actionID: assignment.actionID, attemptID: assignment.attemptID,
        phase, diagnosticID: randomUUID(), code }, { signal: this.#abort.signal, timeoutMs: 5_000 }); }
      catch (error) { this.#notify(error); }
    };
    const report = async (value: ActionReport): Promise<boolean> => {
      const request = { ...this.#identity(), grant, actionID: assignment.actionID, attemptID: assignment.attemptID,
        assignmentID: assignment.assignmentID, phase, report: value };
      if (value.kind === "report" && this.#spool && spoolKey) {
        const key = spoolKey;
        const source: SpoolSource = { ...this.#identity(), grant, runID: assignment.runID, actionID: assignment.actionID,
          attemptID: assignment.attemptID, assignmentID: assignment.assignmentID, phase };
        let lastError = "";
        for (;;) {
          try { await this.#spool.save(key, source, value); spoolStored = true; this.#spoolOutstanding.add(assignment.actionID); break; }
          catch (error) {
            const info = toHarnessError(error, false).info, identity = JSON.stringify([info.code, info.disposition, info.message]);
            if (identity !== lastError) { lastError = identity; this.#notify(error); }
            await this.#spoolDelay();
          }
        }
      }
      const acknowledged = async () => {
        if (spoolStored && spoolKey && this.#spool) for (;;) {
          try { await this.#spool.remove(spoolKey!); if (!(await this.#spool.actionIDs()).includes(assignment.actionID)) this.#spoolOutstanding.delete(assignment.actionID); spoolKey = undefined; spoolStored = false; break; }
          catch (error) { this.#notify(error); await this.#spoolDelay(); }
        }
      };
      const deliver = async (observationOnly: boolean, signal?: AbortSignal) => {
        if (spoolStored && spoolKey && this.#spool && this.environment.sendStagedReport) {
          const retained = await this.#spool.describe(spoolKey);
          if ("chunked" in retained) {
            if (!observationOnly && !lifetime.canOperate()) await verify();
            const { runID: _runID, ...source } = retained.source;
            await this.environment.sendStagedReport(source, await this.#spool.chunks(spoolKey),
              () => ({ grant, observationOnly: observationOnly || this.#isStopped() || !lifetime.canOperate() }), signal);
            return;
          }
        }
        await this.environment.call("reportAction", { ...request, grant, ...(observationOnly ? { observationOnly: true } : {}) },
          { ...(signal ? { signal } : {}), timeoutMs: 5_000 });
      };
      try {
        // An interrupted executor may still return a fact. The original source
        // can submit it as an observation, without regaining execution rights.
        if (this.#isStopped()) await deliver(true);
        else await this.#retry(() => deliver(false, this.#abort.signal));
        await acknowledged(); return true;
      }
      catch (error) {
        // Transport rejection does not make a legal Report a contract violation.
        this.#notify(error);
        if (value.kind !== "report") return false;
        const errorIdentity = (failure: unknown) => {
          const info = toHarnessError(failure, false).info;
          return JSON.stringify([info.code, info.disposition, info.message]);
        };
        let lastError = errorIdentity(error);
        // Keep the original fact and its source in this Action callback. It
        // occupies an action slot and remains visible to stop() until storage
        // is confirmed. The timer is independent of the execution abort:
        // after interrupt or fencing the same source can still report a late
        // observation, but must not regain business authority.
        for (;;) {
          await new Promise<void>(resolve => {
            const timer = setTimeout(resolve, Math.max(1000, this.#session?.pollMs ?? 1000));
            timer.unref();
          });
            if (this.environment.isClosed?.()) {
              if (spoolStored) return false;
              await new Promise<void>(() => {});
            }
          try {
            const observationOnly = this.#isStopped() || !lifetime.canOperate();
            await deliver(observationOnly);
            await acknowledged(); return true;
          } catch (retryError) {
            // Keep the fact on every failed or uncertain retry; in particular,
            // a transport quota cannot turn it into a Capacity diagnostic.
            const identity = errorIdentity(retryError);
            if (identity !== lastError) { lastError = identity; this.#notify(retryError); }
            // A closed Session can never send again. Park without scheduling
            // another timer, while keeping the unfinished Action visible.
            if (retryError instanceof HarnessError && retryError.info.code === "NODE_STATE_INVALID" && retryError.info.message === "Session is closed") {
              if (spoolStored) return false;
              await new Promise<void>(() => {});
            }
          }
        }
      }
    };
    const saveCheckpoint = async (value: Json): Promise<Checkpoint> => {
      if (checkpointBusy) throw harnessError("CHECKPOINT_BUSY", "Another checkpoint save is in flight");
      if (managedBlocked || !lifetime.canOperate()) throw harnessError("EXECUTION_FENCED", "Action Context is not eligible");
      checkpointBusy = true;
      try {
        const operationID = randomUUID(), operationGrant = grant, expectedRevision = checkpoint.revision;
        const request = { ...this.#identity(), grant: operationGrant, actionID: assignment.actionID, attemptID: assignment.attemptID, operationID, expectedRevision, value: copyJson(value) };
        assertJsonValueSize(request.value, this.environment.maxValueBytes);
        await verify();
        try {
          const result = await this.environment.call("checkpointAction", request, { signal, timeoutMs: 5_000 });
          checkpoint = result.checkpoint;
          return copyJson(checkpoint) as unknown as Checkpoint;
        } catch (error) {
          if (!(error instanceof HarnessError) || error.info.disposition !== "uncertain") throw error;
          managedBlocked = true; lifetime.disconnect();
          for (;;) {
            if (signal.aborted || this.#isStopped()) throw harnessError("TRANSPORT_UNCERTAIN", "Original checkpoint confirmation is unresolved", null, "uncertain");
            try {
              const source = { ...this.#identity(), grant: operationGrant, actionID: assignment.actionID, attemptID: assignment.attemptID, operationID };
              const result = this.environment.confirmCheckpoint && expectedRevision < Number.MAX_SAFE_INTEGER
                ? await this.environment.confirmCheckpoint(source, { revision: expectedRevision + 1, value: request.value }, { signal, timeoutMs: 5_000 })
                : await this.environment.call("readCheckpoint", source, { signal, timeoutMs: 5_000 });
              if (result.checkpoint) {
                checkpoint = result.checkpoint;
                await verify(true);
                managedBlocked = false;
                return copyJson(checkpoint) as unknown as Checkpoint;
              }
              // The original caller may retry its idempotent checkpoint request;
              // null is not proof of rejection or permission to change identity.
              // Keep its grant, revision and value fixed, and recheck eligibility
              // even while this unresolved save holds the local operation gate.
              await verify(true);
              const saved = await this.environment.call("checkpointAction", request, { signal, timeoutMs: 5_000 });
              checkpoint = saved.checkpoint;
              managedBlocked = false;
              return copyJson(checkpoint) as unknown as Checkpoint;
            } catch (queryError) {
              if (!(queryError instanceof HarnessError) || queryError.info.disposition !== "uncertain") throw queryError;
              lifetime.disconnect();
            }
            await pause(this.#session!.pollMs);
          }
        }
      } finally { checkpointBusy = false; }
    };
    const definition = this.#definitions.get(key("capacity", assignment.request.target)) as CapacityDefinition<unknown> | undefined;
    const binding = this.#bindings.get(key("capacity", assignment.request.target));
    try {
      for (;;) {
      let dependency: unknown, created = false, invoked = false, continueAfterReconcile = false;
      try {
      if (!definition || !binding) { await diagnostic("VERSION_UNAVAILABLE"); return; }
      await verify();
      if (phase === "reconcile" && !definition.reconcile) { await diagnostic("RECONCILIATION_UNAVAILABLE"); return; }
      try {
        const valid: unknown = definition.validateInput(copyJson(assignment.request.input));
        if (valid && typeof (valid as PromiseLike<unknown>).then === "function") {
          this.#trackAsync(valid as PromiseLike<unknown>, { runID: assignment.runID, actionID: assignment.actionID });
          throw new Error("Validator must be synchronous");
        }
      } catch {
        if (!signal.aborted && lifetime.canOperate()) await diagnostic("INPUT_INVALID");
        return;
      }
      const policies = [...assignment.request.policies, ...(this.#options.policies ?? [])];
      const visited = new Set<string>();
      for (const policy of policies) {
        const identity = canonicalJsonDigest(policy);
        if (visited.has(identity)) continue;
        visited.add(identity);
        const instance = this.#policies.get(key("policy", policy.ref));
        if (!instance) { await diagnostic("VERSION_UNAVAILABLE"); return; }
        if (instance.authorize) {
          let allowed: boolean;
          try {
            allowed = await instance.authorize(copyJson(assignment.request) as unknown as typeof assignment.request,
              { domainID: this.environment.binding.domainID, principalID: assignment.principalID, runID: assignment.runID, rootID: assignment.rootID, config: copyJson(policy.config), phase: phase === "execute" ? "execution" : "reconciliation" });
          } catch {
            if (!signal.aborted && lifetime.canOperate()) await diagnostic("POLICY_FAILED");
            return;
          }
          await verify();
          if (allowed !== true) { await diagnostic("POLICY_REJECTED"); return; }
        }
      }
      try {
        dependency = await binding.create({ runID: assignment.runID, actionID: assignment.actionID, attemptID: assignment.attemptID,
          phase, capacity: { ...assignment.request.target }, config: copyJson(assignment.request.config), signal });
        created = true;
      } catch {
        if (!signal.aborted && lifetime.canOperate()) await diagnostic("DEPENDENCY_CREATE_FAILED");
        return;
      }
      await verify();
      if (this.#spool) {
        let lastError = "";
        for (;;) {
          if (signal.aborted || this.#isStopped()) return;
          try {
            const source: SpoolSource = { ...this.#identity(), grant, runID: assignment.runID, actionID: assignment.actionID,
              attemptID: assignment.attemptID, assignmentID: assignment.assignmentID, phase };
            spoolKey = await this.#spool.reserve(source, reportValueLimit);
            break;
          } catch (error) {
            const info = toHarnessError(error, false).info, identity = JSON.stringify([info.code, info.disposition, info.message]);
            if (identity !== lastError) { lastError = identity; this.#notify(error); }
            await pause(1000);
          }
        }
        await verify();
      }
      if (phase === "execute") {
        if (assignment.mode === "recheck-only") return;
        const dispatchRequestID = randomUUID(), dispatch = { ...this.#identity(), grant, actionID: assignment.actionID, attemptID: assignment.attemptID, dispatchRequestID };
        for (;;) {
          if (signal.aborted || this.#isStopped()) throw harnessError("EXECUTION_FENCED", "Action Context expired before dispatch confirmation");
          try {
            await this.environment.call("dispatchAction", dispatch, { signal, timeoutMs: 5_000 });
            break;
          } catch (error) {
            if (!(error instanceof HarnessError) || error.info.disposition !== "uncertain") throw error;
            lifetime.disconnect();
            for (;;) {
              if (signal.aborted || this.#isStopped()) throw harnessError("TRANSPORT_UNCERTAIN", "Original dispatch confirmation is unresolved", null, "uncertain");
              await pause(this.#session!.pollMs);
              try {
                const reconnected = await lifetime.reconnect(async () => (await this.environment.call("verifyLease", { ...this.#identity(), grant: dispatch.grant }, { signal, timeoutMs: 5_000 })).valid);
                if (!reconnected) throw harnessError("EXECUTION_FENCED", "Action Context cannot reconnect");
                break;
              } catch (verifyError) {
                if (!(verifyError instanceof HarnessError) || verifyError.info.disposition !== "uncertain") throw verifyError;
              }
            }
          }
        }
        await verify();
      }
      const context: CapacityContext<unknown> = { runID: assignment.runID, actionID: assignment.actionID, attemptID: assignment.attemptID,
        attemptNumber: assignment.attemptNumber, signal, config: copyJson(assignment.request.config), deps: dependency,
        checkpoint: copyJson(checkpoint) as unknown as Checkpoint, saveCheckpoint,
        reportProgress: value => { try { this.#options.onProgress?.({ runID: assignment.runID, actionID: assignment.actionID, attemptID: assignment.attemptID, at: Date.now(), value: copyJson(value) }); } catch { /* Observers do not control execution. */ } },
        reportMetric: sample => { try { this.#options.onMetric?.({ runID: assignment.runID, actionID: assignment.actionID, attemptID: assignment.attemptID, at: Date.now(), sample: copyJson(sample) as unknown as typeof sample }); } catch { /* Observers do not control execution. */ } },
      };
      invoked = true;
      try {
        const returned = phase === "execute" ? await definition.execute(copyJson(assignment.request.input), context) : await definition.reconcile!(copyJson(assignment.request.input), context);
        let value: Json;
        try { value = copyJson(returned); }
        catch (error) {
          if (isCopyJsonValidationError(error)) {
            await report({ kind: "diagnostic", code: "RESULT_CONTRACT_VIOLATION", phase, digest: randomUUID() }); return;
          }
          // No immutable snapshot exists. The Capacity may still hold and
          // mutate its returned object, so it must never be sent as a report.
          managedBlocked = true; lifetime.abort();
          this.#notify(harnessError("INTERNAL_ERROR", "Report could not be isolated; its original result is unconfirmed", null, "uncertain"));
          const unisolated = returned;
          await new Promise<void>(() => {});
          void unisolated;
          return;
        }
        try {
          if (this.#spool) {
            const measured = await this.#spool.measure(value, reportValueLimit);
            if (measured.oversized) { await report({ kind: "diagnostic", code: "RESULT_CONTRACT_VIOLATION", phase, digest: measured.digest }); return; }
          } else assertJsonValueSize(value, reportValueLimit);
        }
        catch (error) {
          if (error instanceof HarnessError && error.info.code === "LIMIT_EXCEEDED") {
            await report({ kind: "diagnostic", code: "RESULT_CONTRACT_VIOLATION", phase, digest: createHash("sha256").update(canonicalJson(value)).digest("hex") });
            return;
          }
          // A resource failure during encoding does not establish an oversized
          // Report. Keep the copied value and let report transport retry it.
          this.#notify(error);
        }
        const stored = await report({ kind: "report", value });
        if (stored && phase === "reconcile" && assignment.mode === "automatic-recovery") {
          try { continueAfterReconcile = validateReport(value, "reconcile").payload.kind === "retryable"; } catch { /* The report service retains its diagnostic. */ }
        }
      } catch { await report({ kind: "diagnostic", code: "CAPACITY_THROWN", phase }); }
    } catch (error) {
      if (invoked) await report({ kind: "diagnostic", code: "CAPACITY_THROWN", phase });
      else this.#notify(error);
    } finally {
      if (created && binding?.dispose) {
        try { await binding.dispose(dependency); }
        catch { await diagnostic("DISPOSE_FAILED"); }
      }
    }
      if (!continueAfterReconcile || signal.aborted || this.#isStopped()) return;
      for (;;) {
        await verify();
        const decision = await this.environment.call("actionContinuation", { ...this.#identity(), grant, actionID: assignment.actionID, attemptID: assignment.attemptID }, { signal, timeoutMs: 5_000 });
        if (decision.state === "stop") return;
        await verify();
        if (decision.state === "execute") { phase = "execute"; break; }
        await pause(this.#session!.pollMs);
      }
      }
    } finally {
      if (spoolKey && !spoolStored && this.#spool) {
        try { await this.#spool.remove(spoolKey); } catch (error) { this.#notify(error); }
      }
      finished = true; wake?.(); lifetime.abort(); this.#leases.delete(lifetime); this.#activeGrants.delete(activeGrant); await renew;
    }
  }
  async #acknowledge(assignment: StartAssignment | WakeAssignment | FinalizeAssignment, sentAt: number): Promise<void> {
    const margin = Math.min(Math.floor(assignment.leaseTTLms / 10), assignment.leaseTTLms - 1);
    const lifetime = new LeaseClock(sentAt, assignment.leaseTTLms, margin);
    this.#leases.add(lifetime);
    let grant = assignment.lease, finished = false, wake: (() => void) | undefined;
    const currentGrant = () => grant;
    this.#activeGrants.add(currentGrant);
    const pause = (ms: number) => new Promise<void>(resolve => {
      const done = () => { clearTimeout(timer); lifetime.signal.removeEventListener("abort", done); wake = undefined; resolve(); };
      const timer = setTimeout(done, Math.max(1, ms)); wake = done;
      lifetime.signal.addEventListener("abort", done, { once: true }); if (lifetime.signal.aborted) done();
    });
    const renew = (async () => {
      while (!finished && !lifetime.signal.aborted && !this.#isStopped()) {
        await pause(Math.max(1, Math.floor(assignment.leaseTTLms / 3)));
        if (finished || lifetime.signal.aborted || this.#isStopped()) break;
        const renewRequestID = randomUUID(), renewalSentAt = performance.now();
        for (;;) {
          try {
            const result = await this.environment.call("renewLease", { ...this.#identity(), grant, renewRequestID }, { signal: AbortSignal.any([this.#abort.signal, lifetime.signal]), timeoutMs: 5_000 });
            if (!lifetime.confirmRenewal(renewalSentAt, result.ttlMs)) return;
            grant = result.grant;
            if (!await lifetime.reconnect(async () => true)) return;
            break;
          } catch (error) {
            if (lifetime.signal.aborted || this.#isStopped()) return;
            if (!(error instanceof HarnessError) || error.info.disposition !== "uncertain") { lifetime.abort(); this.#notify(error); return; }
            lifetime.disconnect(); this.#notify(error); await pause(this.#session!.pollMs);
          }
        }
      }
    })();
    try {
      if (!lifetime.canOperate()) return;
      const conclusion = "start" in assignment ? { ok: true as const, started: true as const }
        : "wake" in assignment ? { ok: true as const, woken: true as const } : { ok: true as const, finalized: true as const };
      await this.#reportBusiness(assignment, conclusion, grant, lifetime);
    } finally {
      finished = true; wake?.(); lifetime.abort(); this.#leases.delete(lifetime); this.#activeGrants.delete(currentGrant); await renew;
    }
  }
  async #drive(assignment: DriverAssignment, sentAt: number): Promise<void> {
    const margin = Math.min(Math.floor(assignment.leaseTTLms / 10), assignment.leaseTTLms - 1);
    const lifetime = new LeaseClock(sentAt, assignment.leaseTTLms, margin);
    this.#leases.add(lifetime);
    let grant = assignment.lease, finished = false;
    const activeGrant = () => grant;
    this.#activeGrants.add(activeGrant);
    let wake: (() => void) | undefined;
    const pause = (ms: number) => new Promise<void>(resolve => {
      const done = () => { clearTimeout(timer); lifetime.signal.removeEventListener("abort", done); wake = undefined; resolve(); };
      const timer = setTimeout(done, Math.max(1, ms));
      wake = done;
      lifetime.signal.addEventListener("abort", done, { once: true });
      if (lifetime.signal.aborted) done();
    });
    const renew = (async () => {
      while (!finished && !lifetime.signal.aborted && !this.#isStopped()) {
        await pause(Math.max(1, Math.floor(assignment.leaseTTLms / 3)));
        if (finished || lifetime.signal.aborted || this.#isStopped()) break;
        const renewRequestID = randomUUID(), renewalSentAt = performance.now();
        for (;;) {
          try {
            const result = await this.environment.call("renewLease", { ...this.#identity(), grant, renewRequestID },
              { signal: AbortSignal.any([this.#abort.signal, lifetime.signal]), timeoutMs: 5_000 });
            if (!lifetime.confirmRenewal(renewalSentAt, result.ttlMs)) return;
            grant = result.grant;
            // The authenticated renewal also verifies the original owner and its current eligibility.
            if (!await lifetime.reconnect(async () => true)) return;
            break;
          } catch (error) {
            if (lifetime.signal.aborted || this.#isStopped()) return;
            if (!(error instanceof HarnessError) || error.info.disposition !== "uncertain") { lifetime.abort(); this.#notify(error); return; }
            lifetime.disconnect();
            this.#notify(error);
            await pause(this.#session!.pollMs);
            if (lifetime.signal.aborted || this.#isStopped()) return;
          }
        }
      }
    })();
    try {
      if (!lifetime.canOperate()) return;
      const denied = await this.#authorizeAgentExecution(assignment, () => grant, lifetime);
      const conclusion = denied ?? await this.#advance(assignment);
      if (this.#isStopped() || !lifetime.canOperate()) return;
      await this.#reportBusiness(assignment, conclusion, grant, lifetime);
    } finally {
      finished = true;
      wake?.();
      lifetime.abort();
      this.#leases.delete(lifetime);
      this.#activeGrants.delete(activeGrant);
      await renew;
    }
  }
  /** Agent Action policies belong to its fixed parent request, not to mutable
   * Driver state. Read them through the authenticated current Run lease. */
  async #authorizeAgentExecution(assignment: DriverAssignment, currentGrant: () => BusinessLeaseGrant, lifetime: LeaseClock): Promise<DriverConclusion | null> {
    if (assignment.input.run.parentActionID === null) return null;
    const signal = AbortSignal.any([this.#abort.signal, lifetime.signal]);
    const assertLocal = () => {
      if (signal.aborted || this.#isStopped()) throw harnessError("EXECUTION_FENCED", "Agent execution context is no longer eligible");
    };
    const verify = async () => {
      for (;;) {
        await this.#retry(async () => {
          assertLocal();
          const remote = () => this.environment.call("verifyLease", { ...this.#identity(), grant: currentGrant() }, { signal, timeoutMs: 5_000 });
          if (!lifetime.canOperate()) await lifetime.reconnect(async () => (await remote()).valid);
          else await remote();
        });
        assertLocal();
        // A renewal can supersede this reconnect, or another request can mark
        // the connection uncertain while verification is awaiting its reply.
        if (lifetime.canOperate()) return;
      }
    };
    const execution = await this.#retry(() => {
      assertLocal();
      return this.environment.call("agentExecution", { ...this.#identity(), grant: currentGrant() }, { signal, timeoutMs: 5_000 });
    });
    await verify();
    const request = execution.request;
    if (!request || !execution.actionRunID || execution.runID !== assignment.runID || execution.rootID !== assignment.input.run.rootID || execution.principalID !== assignment.principalID
        || request.kind !== "agent" || canonicalJson(request.target) !== canonicalJson(assignment.input.run.agent)
        || canonicalJson(request.input) !== canonicalJson(assignment.input.run.input) || canonicalJson(request.config) !== canonicalJson(assignment.input.run.config))
      throw harnessError("EXECUTION_FENCED", "Agent execution request does not match its frame");
    const denied = (code: string, message: string): DriverConclusion => ({ ok: false, issue: { code, message, details: { phase: "execution" } } });
    const definition = this.#definitions.get(key("agent", request.target));
    if (!definition) return denied("VERSION_UNAVAILABLE", "Fixed Agent version is not assembled");
    try {
      const result: unknown = definition.validateInput(copyJson(request.input));
      if (result && typeof (result as PromiseLike<unknown>).then === "function") {
        this.#trackAsync(result as PromiseLike<unknown>, { runID: assignment.runID });
        throw new Error("Validator must be synchronous");
      }
    } catch { await verify(); return denied("INVALID_ARGUMENT", "Fixed Agent input validation failed"); }
    const seen = new Set<string>();
    for (const selected of [...request.policies, ...(this.#options.policies ?? [])]) {
      const identity = canonicalJson(selected);
      if (seen.has(identity)) continue;
      seen.add(identity);
      const policy = this.#policies.get(key("policy", selected.ref));
      if (!policy) return denied("VERSION_UNAVAILABLE", "Fixed Agent policy is not assembled");
      if (policy.authorize) {
        let allowed: boolean;
        try {
          allowed = await policy.authorize(copyJson(request) as unknown as typeof request, { domainID: this.environment.binding.domainID, principalID: execution.principalID,
            runID: execution.actionRunID, rootID: execution.rootID, config: copyJson(selected.config), phase: "execution" });
        } catch { await verify(); return denied("STATE_CONFLICT", "Agent policy could not authorize execution"); }
        await verify();
        if (allowed !== true) return denied("FORBIDDEN", "Agent policy rejected execution");
      }
    }
    await verify();
    return null;
  }
  async #reportBusiness(assignment: DriverAssignment | FinalizeAssignment | StartAssignment | WakeAssignment, conclusion: DriverConclusion | FinalizeConclusion | { ok: true; started: true } | { ok: true; woken: true },
    grant: BusinessLeaseGrant, lifetime: LeaseClock): Promise<void> {
    const signal = AbortSignal.any([this.#abort.signal, lifetime.signal]);
    while (!this.#isStopped() && lifetime.canOperate()) {
      try {
        await this.environment.call("report", { ...this.#identity(), assignment, conclusion, grant }, { signal, timeoutMs: 5_000 });
        return;
      } catch (error) {
        if (signal.aborted) return;
        if (!(error instanceof HarnessError) || error.info.disposition !== "uncertain") throw error;
        lifetime.disconnect();
        this.#notify(error);
        for (;;) {
          if (signal.aborted || this.#isStopped()) return;
          await this.#pause(this.#session!.pollMs);
          if (signal.aborted || this.#isStopped()) return;
          try {
            const valid = await lifetime.reconnect(async () => {
              const result = await this.environment.call("verifyLease", { ...this.#identity(), grant }, { signal, timeoutMs: 5_000 });
              return result.valid;
            });
            if (!valid) return;
            break;
          } catch (verifyError) {
            if (signal.aborted || this.#isStopped()) return;
            if (!(verifyError instanceof HarnessError) || verifyError.info.disposition !== "uncertain") {
              lifetime.abort();
              this.#notify(verifyError);
              return;
            }
            this.#notify(verifyError);
          }
        }
      }
    }
  }
  async #advance(assignment: DriverAssignment): Promise<DriverConclusion> {
    const definition = this.#definitions.get(key("agent", assignment.input.run.agent)) as AgentDriver | undefined;
    if (!definition) return { ok: false, issue: { code: "VERSION_UNAVAILABLE", message: "The fixed Agent is not assembled on this node", details: null } };
    try {
      // Fresh data prevents the Driver from modifying assignment evidence.
      const decision = copyJson(await definition.advance(copyJson(assignment.input) as unknown as typeof assignment.input));
      return { ok: true, decision };
    } catch {
      // Application exceptions and non-JSON returns can contain sensitive data.
      return { ok: false, issue: { code: "INVALID_ARGUMENT", message: "The fixed Agent could not produce a decision", details: null } };
    }
  }
  async #validate(assignment: ValidationAssignment): Promise<ValidationConclusion> {
    let payload = assignment.payload;
    if (payload.phase === "report-output") return this.#validateReportOutput(assignment);
    if (payload.phase === "rewrite" || payload.phase === "admission") return this.#prepareAction(assignment);
    const definition = this.#definitions.get(key(payload.kind, payload.ref));
    if (!definition) return { valid: false, issue: { code: "VERSION_UNAVAILABLE", message: "The fixed definition is not assembled on this node", details: null } };
    // Source I/O failures are not output validation failures. Leave the job
    // unresolved so it can be reassigned, instead of publishing valid=false.
    let value = copyJson(payload.value);
    if (payload.kind === "agent" && payload.phase === "output" && record(payload.context) && Object.hasOwn(payload.context, "runConfigSource")) {
      const source = payload.context.runConfigSource;
      if (payload.config !== null || !record(source) || source.encoding !== "run-config-source-v1" || source.runID !== payload.context.runID
          || source.frameID !== payload.context.frameID || source.principalID !== payload.principalID || canonicalJsonDigest(source.agent) !== canonicalJsonDigest(payload.ref)
          || !record(source.value)) throw new Error("Invalid Run config validation source");
      const fields = await this.#readReportValue(assignment, source.value, true, "run-config");
      if (!record(fields) || Object.keys(fields).sort().join(",") !== "config,input") throw new Error("Invalid Run config validation value");
      payload = { ...payload, config: copyJson(fields.config) };
    }
    if (payload.kind === "agent" && payload.phase === "input" && record(payload.context) && Object.hasOwn(payload.context, "submitSource")) {
      const source = payload.context.submitSource;
      if (payload.value !== null || payload.config !== null || !record(source) || source.encoding !== "submit-input-v1" || source.principalID !== payload.principalID
          || !record(payload.context.request) || source.requestKey !== payload.context.request.requestKey || !record(source.value)) throw new Error("Invalid submit validation source");
      const input = await this.#readReportValue(assignment, source.value as { [key: string]: Json }, true);
      if (!record(input) || !fields(input, ["input", "config"])) throw new Error("Invalid submit input value");
      value = copyJson(input.input);
    }

    if (payload.kind === "agent" && payload.phase === "output" && record(payload.context) && Object.hasOwn(payload.context, "driverSource")) {
      const source = payload.context.driverSource;
      if (payload.value !== null || !record(source) || source.encoding !== "driver-conclusion-ref-v1" || source.frameID !== payload.context.frameID || !record(source.value))
        throw new Error("Invalid Driver validation source");
      const report = await this.#readReportValue(assignment, source.value as { [key: string]: Json }, true);
      if (!record(report) || report.ok !== true) throw new Error("Invalid Driver conclusion");
      const decision = validateDecision(report.decision);
      if (decision.kind !== "complete" || decision.outcome.kind !== "succeeded" || canonicalJsonDigest(decision) !== payload.context.decisionDigest)
        throw new Error("Driver validation decision changed");
      value = copyJson(decision.outcome.value);
    }
    try {
      const result: unknown = payload.phase === "input" ? definition.validateInput(value) : definition.validateOutput(value);
      if (result && typeof (result as PromiseLike<unknown>).then === "function") {
        this.#invalidAsyncValidator = true;
        this.#trackAsyncValidation(result as PromiseLike<unknown>, assignment);
        throw new Error("Validators must return synchronously");
      }
      return { valid: true, issue: null };
    } catch {
      // Application exceptions can contain credentials; keep the durable diagnostic finite.
      return { valid: false, issue: { code: payload.phase === "input" ? "INVALID_ARGUMENT" : "OUTPUT_VALIDATION_FAILED",
        message: "The fixed definition rejected the validation value", details: { kind: payload.kind, ref: { ...payload.ref }, phase: payload.phase } } };
    }
  }

  async #readReportValue(assignment: ValidationAssignment, ref: { [key: string]: Json }, driver = false, source?: "policy" | "report" | "action" | "run-config"): Promise<Json> {
    if (!fields(ref, ["id", "owner", "encoding", "length", "digest"]) || !identifier(ref.id) || !identifier(ref.owner)
        || ref.encoding !== "utf8-v1" || typeof ref.length !== "number" || !Number.isSafeInteger(ref.length) || ref.length <= 0
        || typeof ref.digest !== "string" || !/^[a-f0-9]{64}$/.test(ref.digest)) throw new Error("Invalid report value reference");
    const limit = Math.max(1, Math.min(49_152, Math.floor(((this.environment.maxResponseBytes ?? 1024) - 512) * 3 / 4)));
    const decoder = new InternalCanonicalJsonDecoder(), digest = createHash("sha256");
    let offset = 0;
    while (offset < ref.length) {
      const part = await this.environment.call("readValidationValue", { ...this.#identity(), assignment, offset, maxBytes: limit, ...(source ? { source } : {}) }, this.#callOptions());
      if (part.offset !== offset || typeof part.data !== "string") throw new Error("Invalid report value chunk");
      const bytes = Buffer.from(part.data, "base64");
      if (!bytes.length || bytes.length > limit || bytes.toString("base64") !== part.data || bytes.length > ref.length - offset) throw new Error("Invalid report value chunk");
      digest.update(bytes); decoder.write(bytes); offset += bytes.length;
      if (part.done !== (offset === ref.length)) throw new Error("Invalid report value completion");
    }
    if (digest.digest("hex") !== ref.digest) throw new Error("Invalid report value digest");
    const envelope = decoder.finish();
    if (driver) return envelope;
    if (!record(envelope) || !fields(envelope, ["kind", "value"]) || envelope.kind !== "report" || !Object.hasOwn(envelope, "value")) throw new Error("Invalid report value envelope");
    return envelope.value;
  }

  async #expandPolicyPayload(assignment: ValidationAssignment): Promise<Json> {
    const payload = copyJson(assignment.payload) as Record<string, Json>, context = payload.context;
    if (!record(context) || !Object.hasOwn(context, "policySource")) return payload;
    const source = context.policySource;
    if (!record(source) || source.encoding !== "node-policies-v1" || !record(source.value) || !Array.isArray(context.policies)) throw new Error("Invalid policy source");
    const policies = await this.#readReportValue(assignment, source.value, true, "policy");
    if (!Array.isArray(policies)) throw new Error("Invalid policy source value");
    const metadata = policies.map(p => { if (!record(p) || !record(p.ref) || !Object.hasOwn(p, "config")) throw new Error("Invalid policy value"); return { ref: p.ref, config: null }; });
    if (canonicalJsonDigest(metadata) !== canonicalJsonDigest(context.policies)) throw new Error("Policy metadata differs from its source");
    if (record(context.prepared)) {
      if (canonicalJsonDigest(context.prepared.policies) !== canonicalJsonDigest(metadata)) throw new Error("Prepared policy source differs");
      context.prepared.policies = copyJson(policies);
    }
    context.policies = copyJson(policies); delete context.policySource; return payload;
  }

  async #validateReportOutput(assignment: ValidationAssignment): Promise<ValidationConclusion> {
    let payload = expandDispatchPayload(await this.#expandPolicyPayload(assignment)) as unknown as typeof assignment.payload;
    if (record(payload.context) && Object.hasOwn(payload.context, "actionRequestSource")) {
      const context = payload.context, binding = context.actionRequestSource;
      if (!record(binding) || binding.actionID !== context.actionID || !record(binding.source) || binding.source.encoding !== "action-input-config-v1" || !record(binding.source.value)
          || !record(context.prepared) || context.prepared.input !== null || context.prepared.config !== null || payload.config !== null) throw new Error("Invalid Action validation source");
      const fields = await this.#readReportValue(assignment, binding.source.value, true, "action");
      if (!record(fields) || Object.keys(fields).sort().join(",") !== "config,input") throw new Error("Invalid Action validation value");
      context.prepared.input = copyJson(fields.input); context.prepared.config = copyJson(fields.config); delete context.actionRequestSource;
      payload = { ...payload, config: copyJson(fields.config), context };
    }
    if (record(payload.context) && Object.hasOwn(payload.context, "operatorSource")) {
      const source = payload.context.operatorSource;
      if (!record(source) || source.encoding !== "command-value-v1" || !record(source.value) || source.principalID !== payload.principalID || source.actionID !== payload.context.actionID || payload.context.command !== null || payload.value !== null) throw new Error("Invalid operator command source");
      // Source I/O failure leaves the pure job unresolved, never valid=false.
      const request = await this.#readReportValue(assignment, source.value as { [key: string]: Json }, true);
      const command = validateRPCRequest({ protocolVersion: "harness/1", requestID: "operator-source", binding: this.environment.binding, method: "admin.resolveAction", params: request });
      if (command.method !== "admin.resolveAction" || command.params.actionID !== source.actionID || command.params.requestKey !== source.requestKey || (command.params.resolution.kind === "allow_retry" || command.params.resolution.kind === "allow_model_retry")) throw new Error("Operator command source changed");
      const resolution = command.params.resolution;
      payload = { ...payload, value: resolution.kind === "settle" && resolution.result.kind === "succeeded" ? resolution.result.value : null,
        context: copyJson({ ...payload.context, command: command.params, ...(resolution.kind === "settle" ? { originalReport: { payload: resolution.result, evidence: command.params.evidence, measurements: resolution.measurements } } : {}) }) };
    }
    const unavailable = (code: string, message: string): ValidationConclusion => ({ valid: false, issue: { code, message, details: null } });
    const context = payload.context;
    const operator = record(context) && context.reportPhase === "operator";
    if (payload.kind !== "capacity" || !record(context) || context.domainID !== this.environment.binding.domainID
        || !identifier(context.rootID) || !identifier(context.runID) || !identifier(context.frameID) || !identifier(context.actionID)
        || !identifier(context.reportID) || !["execute", "reconcile", "operator"].includes(context.reportPhase as string)
        || (!operator && !identifier(context.attemptID)) || !Array.isArray(context.policies)) return unavailable("INVALID_ARGUMENT", "Invalid report validation binding");
    let prepared: ReturnType<typeof validatePreparedActionRequest>, result: CapacityResult, policies: AppliedPolicy[];
    try {
      prepared = validatePreparedActionRequest(context.prepared);
      policies = context.policies.map(value => {
        if (!record(value) || !fields(value, ["ref", "config"]) || !Object.hasOwn(value, "config")) throw new Error("Invalid policy binding");
        return { ref: checkedRef(value.ref), config: copyJson(value.config) };
      });
      if (new Set(policies.map(policy => key("policy", policy.ref))).size !== policies.length) throw new Error("Duplicate policy");
      const referenced = Object.hasOwn(context, "originalReportRef");
      if (referenced && (context.reportEncoding !== "report-ref-v1" || Object.hasOwn(context, "originalReport") || !record(context.originalReportRef) || payload.value !== null)
          || !referenced && Object.hasOwn(context, "reportEncoding")) throw new Error("Invalid referenced report binding");
      const originalReport = referenced ? await this.#readReportValue(assignment, context.originalReportRef as { [key: string]: Json }, false, Object.hasOwn(context, "operatorSource") ? "report" : undefined) : context.originalReport;
      if (operator) {
        if (payload.operation !== "admin.resolveAction" || !identifier(context.ownerPrincipalID)) throw new Error("Invalid operator authority");
        const command = validateRPCRequest({ protocolVersion: "harness/1", requestID: "operator-validation", binding: { domainID: this.environment.binding.domainID, principalID: payload.principalID }, method: "admin.resolveAction", params: context.command });
        if (command.method !== "admin.resolveAction" || command.params.actionID !== context.actionID || (command.params.resolution.kind === "allow_retry" || command.params.resolution.kind === "allow_model_retry")) throw new Error("Invalid operator command");
        if (command.params.resolution.kind === "settle") {
          if (referenced) throw new Error("Unexpected report reference for operator settlement");
          const report = validateReport(originalReport, "execute");
          if (canonicalJson(originalReport) !== canonicalJson({ payload: command.params.resolution.result, evidence: command.params.evidence, measurements: command.params.resolution.measurements })) throw new Error("Operator report differs from its command");
          result = report.payload;
        } else {
          const resolution = command.params.resolution;
          const digest = referenced ? (context.originalReportRef as { [key: string]: Json }).digest : createHash("sha256").update(canonicalJson({ kind: "report", value: originalReport })).digest("hex");
          if (digest !== resolution.reportDigest) throw new Error("Legacy report differs from its command");
          const report = resolution.phase === "execute" ? validateReport(originalReport, "execute") : validateReport(originalReport, "reconcile");
          const value = resolution.phase === "execute" ? report.payload : report.payload.kind === "settled" ? report.payload.result : null;
          if (!value || !(value.kind === "succeeded" || value.kind === "failed")) throw new Error("Legacy report has no determinate result");
          result = value;
        }
      } else if (context.reportPhase === "execute") result = validateReport(originalReport, "execute").payload;
      else {
        const report = validateReport(originalReport, "reconcile");
        if (report.payload.kind !== "settled") throw new Error("Report has no settled result");
        result = report.payload.result;
      }
      if (prepared.kind !== "capacity" || canonicalJson(prepared.target) !== canonicalJson(payload.ref)
          || canonicalJson(prepared.config) !== canonicalJson(payload.config) || canonicalJsonDigest(prepared.policies) !== canonicalJsonDigest(policies)
          || (!operator && result.kind !== "succeeded") || (!referenced && canonicalJson(result.kind === "succeeded" ? result.value : null) !== canonicalJson(payload.value))) throw new Error("Report output is not bound to its Action");
    } catch { return unavailable("INVALID_ARGUMENT", "Invalid report validation binding"); }
    const definition = this.#definitions.get(key("capacity", prepared.target));
    if (!definition) return unavailable("VERSION_UNAVAILABLE", "Fixed Capacity version unavailable");
    const checkedPolicies = new Set<string>();
    for (const policy of [...policies, ...(this.#options.policies ?? [])]) {
      const policyIdentity = canonicalJsonDigest(policy);
      if (checkedPolicies.has(policyIdentity)) continue;
      checkedPolicies.add(policyIdentity);
      const implementation = this.#policies.get(key("policy", policy.ref));
      if (!implementation) return unavailable("VERSION_UNAVAILABLE", "Fixed policy version unavailable");
      if (!implementation.authorize) continue;
      let allowed: boolean;
      try {
        allowed = await implementation.authorize(copyJson(prepared) as unknown as typeof prepared, {
          domainID: this.environment.binding.domainID, principalID: operator ? context.ownerPrincipalID as string : payload.principalID, runID: context.runID as string,
          rootID: context.rootID as string, config: copyJson(policy.config), phase: context.reportPhase === "execute" ? "execution" : "reconciliation",
        });
        await this.environment.call("verifyValidation", { ...this.#identity(), assignment }, this.#callOptions());
      } catch { return unavailable("STATE_CONFLICT", "Report validation authorization is unavailable"); }
      if (allowed !== true) return unavailable("FORBIDDEN", "Fixed policy rejected report validation");
    }
    try { await this.environment.call("verifyValidation", { ...this.#identity(), assignment }, this.#callOptions()); }
    catch { return unavailable("STATE_CONFLICT", "Report validation is no longer authorized"); }
    if (operator && result.kind === "failed") return { valid: true, issue: null };
    try {
      const output: unknown = definition.validateOutput(copyJson(result.kind === "succeeded" ? result.value : null));
      if (output && typeof (output as PromiseLike<unknown>).then === "function") {
        this.#trackAsyncValidation(output as PromiseLike<unknown>, assignment);
        return unavailable("STATE_CONFLICT", "Output validator must be synchronous");
      }
      return { valid: true, issue: null };
    } catch { return unavailable("OUTPUT_VALIDATION_FAILED", "Fixed Capacity output validation failed"); }
  }

  async #prepareAction(assignment: ValidationAssignment): Promise<ValidationConclusion> {
    const payload = expandDispatchPayload(await this.#expandPolicyPayload(assignment)) as unknown as typeof assignment.payload;
    try {
      const context = payload.context;
      if (!record(context) || context.domainID !== this.environment.binding.domainID || !identifier(context.rootID)
          || !identifier(context.runID) || !identifier(context.frameID) || !Array.isArray(context.policies)) throw new Error("Invalid preparation binding");
      const policies: AppliedPolicy[] = context.policies.map(value => {
        if (!record(value) || !fields(value, ["ref", "config"]) || !Object.hasOwn(value, "config")) throw new Error("Invalid fixed policy");
        return { ref: checkedRef(value.ref), config: copyJson(value.config) };
      });
      if (new Set(policies.map(policy => key("policy", policy.ref))).size !== policies.length) throw new Error("Duplicate policy");
      const definitions = policies.map(policy => {
        const definition = this.#policies.get(key("policy", policy.ref));
        if (!definition) throw new Error("Fixed policy unavailable");
        return definition;
      });
      const boundOriginal = (original: ReturnType<typeof validateActionRequest>) => dispatchOriginalMatches(context, original);
      const policyContext = (config: typeof policies[number]["config"]) => ({ domainID: this.environment.binding.domainID,
        principalID: payload.principalID, runID: context.runID as string, rootID: context.rootID as string, config: copyJson(config), phase: "admission" as const });
      if (payload.phase === "rewrite") {
        if (payload.kind !== "agent" || !Array.isArray(payload.value) || payload.config !== null) throw new Error("Invalid rewrite payload");
        const originals = payload.value.map(validateActionRequest);
        if (new Set(originals.map(action => action.key)).size !== originals.length || originals.some(action => !boundOriginal(action))) throw new Error("Unbound rewrite request");
        const rewritten = originals.map(original => {
          let current = original;
          for (let index = 0; index < definitions.length; index++) {
            const rewrite = definitions[index]!.rewrite;
            if (!rewrite) continue;
            const result: unknown = rewrite(copyJson(current) as unknown as typeof current, policyContext(policies[index]!.config));
            if (result && typeof (result as PromiseLike<unknown>).then === "function") {
              this.#trackAsyncValidation(result as PromiseLike<unknown>, assignment);
              throw new Error("Rewrite must be synchronous");
            }
            current = validateActionRequest(copyJson(result));
            if (current.key !== original.key || current.kind !== original.kind) throw new Error("Rewrite changed action identity");
            assertJsonValueSize(current.input, this.environment.maxValueBytes);
            assertJsonValueSize(current.config, this.environment.maxValueBytes);
          }
          return current;
        });
        return { valid: true, issue: null, rewritten };
      }
      if (payload.phase !== "admission") throw new Error("Invalid admission payload");
      const original = validateActionRequest(context.original), prepared = validatePreparedActionRequest(context.prepared);
      if (!boundOriginal(original) || original.key !== prepared.key || original.kind !== prepared.kind || prepared.kind !== payload.kind
          || canonicalJsonDigest(prepared.policies) !== canonicalJsonDigest(policies) || canonicalJson(prepared.target) !== canonicalJson(payload.ref)
          || canonicalJson(prepared.input) !== canonicalJson(payload.value) || canonicalJson(prepared.config) !== canonicalJson(payload.config)) throw new Error("Unbound admission request");
      const definition = this.#definitions.get(key(prepared.kind, prepared.target));
      if (!definition) throw new Error("Fixed definition unavailable");
      const validation: unknown = definition.validateInput(copyJson(prepared.input));
      if (validation && typeof (validation as PromiseLike<unknown>).then === "function") {
        this.#trackAsyncValidation(validation as PromiseLike<unknown>, assignment);
        throw new Error("Validator must be synchronous");
      }
      for (let index = 0; index < definitions.length; index++) {
        const authorize = definitions[index]!.authorize;
        if (authorize && await authorize(copyJson(prepared) as unknown as typeof prepared, policyContext(policies[index]!.config)) !== true)
          return { valid: false, issue: { code: "FORBIDDEN", message: "A fixed policy rejected the action", details: null } };
      }
      return { valid: true, issue: null };
    } catch {
      return { valid: false, issue: { code: payload.phase === "rewrite" ? "INVALID_DECISION" : "INVALID_ARGUMENT", message: "The fixed preparation failed", details: null } };
    }
  }
}

function positive(value: unknown): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value > 0; }
function remaining(deadline: number): number { return Math.max(0, Math.ceil(deadline - performance.now())); }
async function within(promise: Promise<unknown>, timeoutMs: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = performance.now() + timeoutMs;
  try {
    return await Promise.race([promise.then(() => true), new Promise<false>(resolve => {
      const check = () => {
        const left = deadline - performance.now();
        if (left <= 0) resolve(false);
        else timer = setTimeout(check, Math.min(left, 2_147_483_647));
      };
      timer = setTimeout(check, Math.min(timeoutMs, 2_147_483_647));
    })]);
  } finally { if (timer) clearTimeout(timer); }
}
function checkedRef(value: unknown): Ref {
  if (!record(value) || !fields(value, ["id", "version"]) || !identifier(value.id) || !identifier(value.version)) throw harnessError("INVALID_ARGUMENT", "A fixed definition Ref is required");
  return Object.freeze({ id: value.id, version: value.version });
}
