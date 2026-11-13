import { nodeStopSnapshot } from "../contract/index.js";
import { createHash, randomUUID } from "node:crypto";
import { Agent as HTTPAgent, request as httpRequest } from "node:http";
import { Agent as HTTPSAgent, request as httpsRequest } from "node:https";
import { constants as bufferConstants } from "node:buffer";
import { carriesNodeData, nodeDataLimit } from "./limits.js";
import { CanonicalNodeFragmentSender } from "./canonical-node-fragment-sender.js";
import { checkpointDigest } from "./checkpoint-echo.js";
import { NODE_PROTOCOL_CAPABILITY, NODE_PROTOCOL_HEADER } from "./node-protocol-version.js";
import { canonicalJsonChunks } from "../contract/index.js";
import { CanonicalFragmentSender } from "./canonical-fragment-sender.js";
import { DurableReportFragmentSender, type DurableReportChunks } from "./durable-report-fragment-sender.js";
import { BoundReportTransferClient } from "./bound-report-transfer-client.js";
import {
  HarnessError, harnessError, parseJson, canonicalJson, canonicalJsonDigest, InternalCanonicalJsonDecoder, validateRPCRequest, validateRPCResponse, validateNodeRequest, validateNodeResponse,
  type CallOptions, type ConnectOptions, type DomainInfo, type HarnessSession,
  type NodeStopResult, type PublicMethod, type PublicRPCMap, type SessionBinding,
  type HarnessNode, type NodeOptions, type NodeCall, type NodeMethod, type NodeRPCMap, type NodeAssignment, type Checkpoint,
} from "../contract/index.js";

export interface NodeEnvironment {
  isClosed?(): boolean;
  binding: SessionBinding;
  call: NodeCall;
  maxValueBytes: number;
  maxResponseBytes?: number;
  confirmCheckpoint?(params: NodeRPCMap["readCheckpoint"]["params"], expected: Checkpoint, options?: CallOptions): Promise<NodeRPCMap["readCheckpoint"]["result"]>;
  sendStagedReport?(source: Omit<NodeRPCMap["reportAction"]["params"], "report" | "observationOnly">, chunks: DurableReportChunks,
    current: () => { grant: NodeRPCMap["reportAction"]["params"]["grant"]; observationOnly: boolean }, signal?: AbortSignal): Promise<{ stored: true } | { stored: false; diagnostic: true }>;
}
export type NodeFactory = (options: NodeOptions, environment: NodeEnvironment) => HarnessNode;
let nodeFragmentThresholdForTest: number | undefined;
/** Test representation threshold only; does not change advertised limits. */
export function setNodeFragmentThresholdForTest(bytes: number): () => void {
  if (!Number.isSafeInteger(bytes) || bytes <= 0 || bytes > 1_048_576 || nodeFragmentThresholdForTest !== undefined) throw new Error("Invalid node fragment test threshold");
  nodeFragmentThresholdForTest = bytes; return () => { nodeFragmentThresholdForTest = undefined; };
}

/** A session owns its HTTP connection pool; it never owns or cancels a business Run. */
export async function connect(options: ConnectOptions, nodeFactory?: NodeFactory): Promise<HarnessSession> {
  if (!options || typeof options !== "object" || typeof options.endpoint !== "string"
      || typeof options.tokenProvider !== "function" || Object.keys(options).some(key => !["endpoint", "tokenProvider"].includes(key))) {
    throw harnessError("INVALID_ARGUMENT", "An endpoint and tokenProvider are required");
  }
  let endpoint: URL;
  try { endpoint = new URL(options.endpoint); }
  catch { throw harnessError("INVALID_ARGUMENT", "Invalid coordinator endpoint"); }
  if (!["http:", "https:"].includes(endpoint.protocol) || endpoint.username || endpoint.password
      || endpoint.search || endpoint.hash || endpoint.pathname !== "/") {
    throw harnessError("INVALID_ARGUMENT", "Endpoint must be an HTTP(S) service root URL");
  }
  endpoint.pathname = "/v1/rpc";
  const transport = new Transport(endpoint, options.tokenProvider);
  let info: DomainInfo;
  try {
    info = await transport.call("session.getInfo", {}, null);
    transport.maxNodeReportBytes = nodeDataLimit(info.maxPayloadBytes, info.maxValueBytes, info.maxResponseBytes);
  }
  catch (error) { transport.close(); throw error; }
  const binding = Object.freeze({ domainID: info.domainID, principalID: info.principalID });
  transport.maxPayloadBytes = info.maxPayloadBytes;
  transport.maxResponseBytes = info.maxResponseBytes;
  transport.maxNodeResponseBytes = info.maxResponseBytes;
  let closed = false;
  let closeReport: NodeStopResult | undefined;
  let closing: Promise<NodeStopResult> | undefined;
  const nodes = new Set<HarnessNode>();
  const call = <M extends PublicMethod>(method: M, params: PublicRPCMap[M]["params"], callOptions?: CallOptions) => {
    if (closed) return Promise.reject(harnessError("NODE_STATE_INVALID", "Session is closed"));
    return transport.call(method, params, binding, callOptions);
  };
  return Object.freeze({
    binding,
    getInfo: (callOptions?: CallOptions) => call("session.getInfo", {}, callOptions),
    client: Object.freeze({
      submit: (request, opts) => call("client.submit", request, opts),
      getRun: (request, opts) => call("client.getRun", request, opts),
      getRunByRequestKey: (request, opts) => call("client.getRunByRequestKey", request, opts),
      getAction: (request, opts) => call("client.getAction", request, opts),
      listActions: (request, opts) => call("client.listActions", request, opts),
      requestCancel: (request, opts) => call("client.requestCancel", request, opts),
      getCommand: (request, opts) => call("client.getCommand", request, opts),
      getVersion: (request, opts) => call("client.getVersion", request, opts),
      getHistory: (request, opts) => call("client.getHistory", request, opts),
    } satisfies HarnessSession["client"]),
    admin: Object.freeze({
      resumeRun: (request, opts) => call("admin.resumeRun", request, opts),
      recheckAction: (request, opts) => call("admin.recheckAction", request, opts),
      resolveAction: (request, opts) => call("admin.resolveAction", request, opts),
      adoptVersion: (request, opts) => call("admin.adoptVersion", request, opts),
    } satisfies HarnessSession["admin"]),
    createNode(nodeOptions: NodeOptions) {
      if (closed) throw harnessError("NODE_STATE_INVALID", "Session is closed");
      if (!nodeFactory) throw harnessError("STATE_CONFLICT", "Node assembly is not configured");
      const node = nodeFactory(nodeOptions, { binding, maxValueBytes: info.maxValueBytes, maxResponseBytes: info.maxResponseBytes, isClosed: () => closed,
        call: (method, params, callOptions) => transport.nodeCall(method, params, binding, callOptions),
        confirmCheckpoint: (params, expected, callOptions) => transport.nodeCall("readCheckpoint", params, binding, callOptions, expected),
        ...(transport.supportsStagedReports ? { sendStagedReport: (source, chunks, current, signal) => transport.sendStagedReport(source, chunks, current, binding, signal) } : {}) });
      nodes.add(node);
      return node;
    },
    async close(closeOptions?: { timeoutMs?: number }): Promise<NodeStopResult> {
      if (closeReport) return structuredClone(closeReport);
      if (closeOptions !== undefined && (closeOptions === null || typeof closeOptions !== "object" || Array.isArray(closeOptions) || Object.keys(closeOptions).some(key => key !== "timeoutMs"))) {
        throw harnessError("INVALID_ARGUMENT", "Invalid close options");
      }
      duration(closeOptions?.timeoutMs);
      if (!closing) {
        closed = true;
        const timeoutMs = closeOptions?.timeoutMs ?? 5_000;
        closing = (async () => {
          const reports = new Map<HarnessNode, NodeStopResult>();
          let failed = false;
          const stopping = Promise.all([...nodes].map(async node => {
            try { reports.set(node, await node.stop({ mode: "interrupt", timeoutMs })); }
            catch { failed = true; }
          }));
          const completed = await beforeTimeout(stopping, timeoutMs);
          transport.close();
          const snapshots = [...nodes].map(node => reports.get(node) ?? (node as HarnessNode & { [nodeStopSnapshot]?: () => NodeStopResult })[nodeStopSnapshot]?.()
            ?? { stopped: false, remainingRunIDs: [], remainingActionIDs: [] });
          closeReport = { stopped: completed && !failed && snapshots.every(report => report.stopped),
            remainingRunIDs: [...new Set(snapshots.flatMap(report => report.remainingRunIDs))],
            remainingActionIDs: [...new Set(snapshots.flatMap(report => report.remainingActionIDs))] };
          // An incomplete stop can still hold an unconfirmed report. Keep its
          // node reachable for as long as this Session remains reachable.
          if (closeReport.stopped) nodes.clear();
          return closeReport;
        })();
      }
      return structuredClone(await closing);
    },
  } satisfies HarnessSession);
}

class Transport {
  readonly #agent: HTTPAgent | HTTPSAgent;
  readonly #pending = new Set<() => void>();
  // Unconfirmed Node uploads retain their original request identity and the
  // last acknowledged offset across calls. This is bounded transient state;
  // the node's durable spool remains the source of truth after process loss.
  #preparingUploads = 0;
  readonly #uploads = new Map<string, { requestID: string; sender: CanonicalNodeFragmentSender; offset: number; active: boolean; expires: number }>();
  #closed = false;
  maxPayloadBytes = 1_048_576;
  maxNodeReportBytes = 1_048_576;
  maxNodeResponseBytes = 4_194_304;
  supportsNodeFragments = false;
  supportsReportFinalize = false;
  supportsNodeProtocol = false;
  supportsStagedReports = false;
  // Legacy coordinators do not advertise a response budget in the HTTP header.
  // Keep the historical bound for their first handshake.
  maxResponseBytes = 4_194_304;

  constructor(readonly endpoint: URL, readonly tokenProvider: ConnectOptions["tokenProvider"]) {
    this.#agent = endpoint.protocol === "https:" ? new HTTPSAgent({ keepAlive: true }) : new HTTPAgent({ keepAlive: true });
  }

  call<M extends PublicMethod>(method: M, params: PublicRPCMap[M]["params"], binding: SessionBinding | null, options?: CallOptions): Promise<PublicRPCMap[M]["result"]> {
    return this.exchange(method, params, binding, options, false) as Promise<PublicRPCMap[M]["result"]>;
  }

  nodeCall<M extends NodeMethod>(method: M, params: NodeRPCMap[M]["params"], binding: SessionBinding, options?: CallOptions, expectedCheckpoint?: Checkpoint): Promise<NodeRPCMap[M]["result"]> {
    if (method === "register" && !this.supportsNodeProtocol) return Promise.reject(harnessError("PROTOCOL_MISMATCH", "Coordinator does not support the required node protocol"));
    return this.exchange(method, params, binding, options, true, expectedCheckpoint) as Promise<NodeRPCMap[M]["result"]>;
  }

  async sendStagedReport(source: Omit<NodeRPCMap["reportAction"]["params"], "report" | "observationOnly">, chunks: DurableReportChunks,
    current: () => { grant: NodeRPCMap["reportAction"]["params"]["grant"]; observationOnly: boolean }, binding: SessionBinding, signal?: AbortSignal): Promise<{ stored: true } | { stored: false; diagnostic: true }> {
    if (!this.supportsStagedReports || this.#closed) throw harnessError("PROTOCOL_MISMATCH", "Coordinator does not support staged reports");
    const hash = createHash("sha256"); let identityChunks = 0;
    for (const part of canonicalJsonChunks([binding.domainID, binding.principalID, source.nodeID, source.incarnation,
      source.actionID, source.attemptID, source.phase, source.grant.epoch, chunks.key])) {
      hash.update(part);
      if (++identityChunks % 16 === 0) await new Promise<void>(resolve => setImmediate(resolve));
    }
    const transferID = hash.digest("hex");
    const request = { protocolVersion: "harness/1", requestID: `report-v4:${transferID}`, binding, method: "reportAction", params: { ...source, report: { kind: "report", value: null } } };
    const checked = validateNodeRequest(request);
    if (checked.method !== "reportAction") throw harnessError("INVALID_ARGUMENT", "Invalid staged report source");
    const control = await CanonicalFragmentSender.create(canonicalJsonChunks({ request: checked, transferID, length: chunks.length, digest: chunks.digest }), 1_073_741_824, transferID);
    const report = new DurableReportFragmentSender(chunks, transferID);
    const endpoint = new URL(this.endpoint); endpoint.pathname = "/";
    const client = new BoundReportTransferClient(endpoint.href, this.tokenProvider, source.nodeID, binding);
    const frameSignal = () => signal ? AbortSignal.any([signal, AbortSignal.timeout(5_000)]) : AbortSignal.timeout(5_000);
    for (let offset = 0; offset < control.total;) {
      const ack = await client.control(control, offset, frameSignal()); offset = ack.nextOffset;
      if (offset === control.total && !ack.bound) throw harnessError("TRANSPORT_UNCERTAIN", "Report source binding was not confirmed", null, "uncertain");
    }
    let diagnostic = false;
    for (let offset = 0; offset < report.total;) {
      const finalOffset = offset + Math.min(65_536, report.total - offset) === report.total;
      let final: { expiresAt: number; observationOnly: boolean } | undefined;
      if (finalOffset) {
        const state = current(), initial = source.grant, grant = state.grant;
        if (!grant || grant.kind !== initial.kind || grant.subjectID !== initial.subjectID || grant.nodeID !== initial.nodeID
            || grant.incarnation !== initial.incarnation || grant.workerPrincipalID !== initial.workerPrincipalID || grant.epoch !== initial.epoch
            || grant.attemptID !== initial.attemptID || grant.attemptNumber !== initial.attemptNumber || typeof state.observationOnly !== "boolean")
          throw harnessError("EXECUTION_FENCED", "Report source grant changed");
        final = { expiresAt: grant.expiresAt, observationOnly: state.observationOnly };
      }
      const ack = await client.report(report, offset, final, frameSignal()); offset = ack.nextOffset;
      if (offset === report.total && !ack.stored && !ack.diagnostic) throw harnessError("TRANSPORT_UNCERTAIN", "Report processing was not confirmed", null, "uncertain");
      if (offset === report.total) diagnostic = ack.diagnostic === true;
    }
    return diagnostic ? { stored: false, diagnostic: true } : { stored: true };
  }

  private async exchange(method: PublicMethod | NodeMethod, params: unknown, binding: SessionBinding | null, options: CallOptions | undefined, internal: boolean, knownCheckpoint?: Checkpoint): Promise<unknown> {
    if (options !== undefined && (options === null || typeof options !== "object" || Array.isArray(options) || Object.keys(options).some(key => !["signal", "timeoutMs"].includes(key)))) {
      throw harnessError("INVALID_ARGUMENT", "Invalid call options");
    }
    // A caller may reuse the options object while this call is pending. Keep the
    // original signal for both listener registration and cleanup.
    const signal = options?.signal;
    const timeoutMs = options?.timeoutMs;
    duration(timeoutMs);
    const startedAt = performance.now();
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw harnessError("INVALID_ARGUMENT", "Invalid abort signal");
    // Validation and copying happen before the token provider or any network I/O.
    let requestID: string = randomUUID();
    const candidate = { protocolVersion: "harness/1", requestID, binding, method, params };
    let request = internal ? validateNodeRequest(candidate) : validateRPCRequest(candidate);
    const expectedCheckpoint = internal && request.method === "checkpointAction" && request.params.expectedRevision < Number.MAX_SAFE_INTEGER
      ? { revision: request.params.expectedRevision + 1, value: request.params.value } : internal && request.method === "readCheckpoint" ? knownCheckpoint : undefined;
    let checkpointHint = binding && expectedCheckpoint && (request.method === "checkpointAction" || request.method === "readCheckpoint")
      ? checkpointDigest(requestID, binding, request.params.operationID, expectedCheckpoint) : undefined;
    const requestLimit = internal && carriesNodeData(method) ? this.maxNodeReportBytes : this.maxPayloadBytes;
    const threshold = Math.min(requestLimit, nodeFragmentThresholdForTest ?? 1_048_576);
    const canonicalNode = internal && carriesNodeData(method) && this.supportsNodeFragments;
    let encodedBytes = 0;
    if (canonicalNode) for (const part of canonicalJsonChunks(request)) {
      encodedBytes += part.length;
      if (encodedBytes > threshold) break;
      if (signal?.aborted || this.#closed || timeoutMs !== undefined && performance.now() - startedAt >= timeoutMs) throw harnessError("CALL_ABORTED", "RPC wait ended before sending");
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    const nodeFragments = canonicalNode && encodedBytes > threshold;
    let body = nodeFragments ? "" : canonicalJson(request);
    const finalizableReport = nodeFragments && request.method === "reportAction" && this.supportsReportFinalize;
    if (!nodeFragments && Buffer.byteLength(body) > requestLimit) throw harnessError("LIMIT_EXCEEDED", "RPC request exceeds transport limit");
    if (this.#closed) throw harnessError("NODE_STATE_INVALID", "Session is closed");
    let uploadKey: string | undefined;
    let upload: { requestID: string; sender: CanonicalNodeFragmentSender; offset: number; active: boolean; expires: number } | undefined;
    const assertPreparing = () => { if (signal?.aborted || this.#closed || timeoutMs !== undefined && performance.now() - startedAt >= timeoutMs) throw harnessError("CALL_ABORTED", "RPC wait ended before sending"); };
    if (nodeFragments && this.#preparingUploads >= 8) throw harnessError("TRANSPORT_UNCERTAIN", "Node upload preparation is busy", null, "uncertain");
    if (nodeFragments) this.#preparingUploads++;
    try {
      if (nodeFragments && method === "reportAction" && !finalizableReport) {
        // A resumed old grant can expire while the same epoch has been renewed.
        // Replaying it could record an observation instead of the authoritative
        // report. Keep this call's original bounded deadline until finalization
        // can separately bind a current grant without changing report identity.
        upload = { requestID, sender: await CanonicalNodeFragmentSender.create(request, Math.min(requestLimit, 1_073_741_824), assertPreparing), offset: 0, active: true, expires: 0 };
      } else if (nodeFragments) {
        const identityRequest = finalizableReport && request.method === "reportAction"
          ? { ...request, requestID: "", params: { ...request.params, grant: { ...request.params.grant, expiresAt: 0 }, observationOnly: false } }
          : { ...request, requestID: "" };
        uploadKey = (await CanonicalFragmentSender.create(canonicalJsonChunks(identityRequest), 1_073_741_824, undefined, assertPreparing)).digest;
        const now = Date.now();
        for (const [key, item] of this.#uploads) if (!item.active && item.expires <= now) this.#uploads.delete(key);
        upload = this.#uploads.get(uploadKey);
        if (upload?.active) throw harnessError("TRANSPORT_UNCERTAIN", "Identical node upload is already in progress", null, "uncertain");
        if (!upload) {
          let reserved = 0; for (const item of this.#uploads.values()) reserved += item.sender.total;
          if (this.#uploads.size >= 8 || encodedBytes > 1_073_741_824 - reserved)
            throw harnessError("TRANSPORT_UNCERTAIN", "Node upload staging is temporarily full", null, "uncertain");
          upload = { requestID, sender: await CanonicalNodeFragmentSender.create(request, Math.min(requestLimit, 1_073_741_824 - reserved), assertPreparing), offset: 0, active: false, expires: now + 120_000 };
          let currentReserved = 0; for (const item of this.#uploads.values()) currentReserved += item.sender.total;
          if (this.#uploads.has(uploadKey) || this.#uploads.size >= 8 || upload.sender.total > 1_073_741_824 - currentReserved) throw harnessError("TRANSPORT_UNCERTAIN", "Node upload staging changed while preparing", null, "uncertain");
          this.#uploads.set(uploadKey, upload);
        } else {
          requestID = upload.requestID;
          request = { ...request, requestID };
          // The cached sender owns this exact request ID and immutable bytes.
          if (binding && expectedCheckpoint && (request.method === "checkpointAction" || request.method === "readCheckpoint"))
            checkpointHint = checkpointDigest(requestID, binding, request.params.operationID, expectedCheckpoint);
        }
        upload.active = true; upload.expires = now + 120_000;
      }
    } finally { if (nodeFragments) this.#preparingUploads--; }
    if (signal?.aborted || this.#closed) { if (upload) upload.active = false; throw harnessError("CALL_ABORTED", "RPC wait ended before sending"); }
    return new Promise((resolve, reject) => {
      let sent = false;
      let settled = false;
      let network: ReturnType<typeof httpRequest> | undefined;
      const fragmentRequests = new Set<ReturnType<typeof httpRequest>>();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const finish = (error?: unknown, value?: unknown) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        signal?.removeEventListener("abort", abort);
        this.#pending.delete(abort);
        if (upload) { upload.active = false; upload.expires = Date.now() + 120_000; if (!error && uploadKey) this.#uploads.delete(uploadKey); }
        if (error) { reject(error); network?.destroy(); for (const pending of fragmentRequests) pending.destroy(); }
        else resolve(value!);
      };
      const uncertain = () => finish(harnessError("TRANSPORT_UNCERTAIN", "A valid RPC response was not confirmed"));
      const abort = () => finish(sent
        ? harnessError("TRANSPORT_UNCERTAIN", "RPC wait ended after the request may have been sent")
        : harnessError("CALL_ABORTED", "RPC wait ended before sending"));
      this.#pending.add(abort);
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted || timeoutMs === 0) { abort(); return; }
      if (timeoutMs !== undefined) {
        const started = startedAt;
        const timeout = timeoutMs;
        const wait = () => {
          const remaining = timeout - (performance.now() - started);
          if (remaining <= 0) abort();
          else timer = setTimeout(wait, Math.min(remaining, 2_147_483_647));
        };
        wait();
      }
      void (async () => {
        let token: string;
        try { token = await this.tokenProvider(); }
        catch { finish(harnessError("UNAUTHENTICATED", "Credential provider failed before sending")); return; }
        if (settled) return;
        if (typeof token !== "string" || !/^[\x21-\x7e]+$/.test(token)) {
          finish(harnessError("UNAUTHENTICATED", "Credential provider returned an invalid bearer token")); return;
        }
        try {
          const send = this.endpoint.protocol === "https:" ? httpsRequest : httpRequest;
          const target = new URL(this.endpoint);
          if (internal) target.pathname = "/v1/node";
          const onResponse = (response: import("node:http").IncomingMessage) => {
            if (checkpointHint && expectedCheckpoint && binding && (request.method === "checkpointAction" || request.method === "readCheckpoint")
                && response.headers["harness-node-checkpoint-echo-version"] === "2") {
              void (async () => {
                const chunks: Buffer[] = []; let bytes = 0;
                for await (const chunk of response) { const value = Buffer.from(chunk); bytes += value.length; if (bytes > this.maxNodeResponseBytes) throw new Error("Checkpoint acknowledgement exceeds budget"); chunks.push(value); }
                if (!response.complete || response.statusCode !== 200 || !/^application\/json(?:\s*;|$)/i.test(response.headers["content-type"] ?? "")) throw new Error("Invalid checkpoint acknowledgement");
                const value = parseJson(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(Buffer.concat(chunks)));
                if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).sort().join(",") !== "binding,ok,protocolVersion,requestID,result"
                    || value.protocolVersion !== "harness/1" || value.requestID !== requestID || value.ok !== true
                    || !value.binding || typeof value.binding !== "object" || Array.isArray(value.binding)
                    || Object.keys(value.binding).sort().join(",") !== "domainID,principalID" || !sameBinding(value.binding as unknown as SessionBinding, binding)
                    || !value.result || typeof value.result !== "object" || Array.isArray(value.result)
                    || Object.keys(value.result).sort().join(",") !== "digest,operationID" || value.result.operationID !== request.params.operationID
                    || value.result.digest !== checkpointHint) throw new Error("Unbound checkpoint acknowledgement");
                const envelope = validateNodeResponse({ protocolVersion: "harness/1", requestID, binding, ok: true,
                  result: { checkpoint: expectedCheckpoint } }, request.method);
                if (!envelope.ok) throw new Error("Invalid checkpoint receipt");
                finish(undefined, envelope.result);
              })().catch(uncertain);
              return;
            }
            if (internal && request.method === "report" && response.headers["harness-node-echo-version"] === "2") {
              void (async () => {
                const chunks: Buffer[] = []; let bytes = 0;
                for await (const chunk of response) { const value = Buffer.from(chunk); bytes += value.length; if (bytes > this.maxNodeResponseBytes) throw new Error("Node acknowledgement exceeds budget"); chunks.push(value); }
                if (!response.complete || response.statusCode !== 200 || !/^application\/json(?:\s*;|$)/i.test(response.headers["content-type"] ?? "")) throw new Error("Invalid node acknowledgement");
                const value = parseJson(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(Buffer.concat(chunks)));
                if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).sort().join(",") !== "binding,ok,protocolVersion,requestID,result"
                    || value.protocolVersion !== "harness/1" || value.requestID !== requestID || value.ok !== true
                    || !value.binding || typeof value.binding !== "object" || Array.isArray(value.binding)
                    || Object.keys(value.binding).sort().join(",") !== "domainID,principalID" || !binding || !sameBinding(value.binding as unknown as SessionBinding, binding)
                    || !value.result || typeof value.result !== "object" || Array.isArray(value.result) || Object.keys(value.result).join(",") !== "digest"
                    || value.result.digest !== canonicalJsonDigest(request.params.conclusion)) throw new Error("Unbound node acknowledgement");
                const envelope = validateNodeResponse({ protocolVersion: "harness/1", requestID, binding, ok: true,
                  result: { conclusion: request.params.conclusion } }, "report");
                if (!envelope.ok) throw new Error("Invalid node report receipt");
                finish(undefined, envelope.result);
              })().catch(uncertain);
              return;
            }
            if ((!internal || ["claim", "checkpointAction", "readCheckpoint", "readValidationValue"].includes(method)) && response.headers["harness-fragment-version"] === "2") {
              let cleanupID = "", cleanupToken = token;
              const releaseSnapshot = () => {
                if (!cleanupID) return;
                try {
                  const releaseBody = canonicalJson({ id: cleanupID, release: true });
                  const releaseTarget = new URL(this.endpoint); releaseTarget.pathname = "/v2/response-fragment";
                  const releaseRequest = send(releaseTarget, { method: "POST", agent: this.#agent, headers: { "content-type": "application/json; charset=utf-8",
                    authorization: `Bearer ${cleanupToken}`, "content-length": Buffer.byteLength(releaseBody) } }, incoming => incoming.resume());
                  releaseRequest.on("error", () => {}); releaseRequest.end(releaseBody);
                } catch { /* Idle expiry remains the fallback. */ }
              };
              void (async () => {
                const hash = createHash("sha256");
                const canonicalStream = response.headers["harness-fragment-json"] === "canonical-1";
                const decoder = canonicalStream ? new InternalCanonicalJsonDecoder() : new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
                const parts: string[] = [];
                const decode = (chunk: Uint8Array) => {
                  if (decoder instanceof InternalCanonicalJsonDecoder) decoder.write(chunk);
                  else parts.push(decoder.decode(chunk, { stream: true }));
                };
                let current = response, offset = 0, id = "", digest = "", total = 0, firstLength = 0;
                const fragmentByteLimit = internal ? this.maxNodeResponseBytes : this.maxResponseBytes;
                const nodeRequestBudget = internal ? response.headers["harness-node-max-request-bytes"] : undefined;
                const nodeResponseBudget = internal ? response.headers["harness-node-max-response-bytes"] : undefined;
                const nodeVersion = internal ? response.headers["harness-node-fragment-version"] : undefined;
                const reportFinalizeVersion = internal ? response.headers["harness-node-report-finalize-version"] : undefined;
                const validNodeBudget = (value: unknown): value is string => typeof value === "string" && /^[1-9][0-9]*$/.test(value)
                  && Number.isSafeInteger(Number(value)) && Number(value) <= Number.MAX_SAFE_INTEGER;
                if (internal && (!validNodeBudget(nodeRequestBudget) || !validNodeBudget(nodeResponseBudget) || nodeVersion !== "2"
                    || reportFinalizeVersion !== undefined && reportFinalizeVersion !== "3")) throw new Error("Invalid node fragment budgets");
                for (;;) {
                  if (settled || current.statusCode !== 200 || current.headers["harness-fragment-version"] !== "2"
                      || !/^application\/octet-stream(?:\s*;|$)/i.test(current.headers["content-type"] ?? "")) throw new Error("Invalid fragment response");
                  const fragmentID = current.headers["harness-fragment-id"], fragmentOffset = current.headers["harness-fragment-offset"],
                    fragmentTotal = current.headers["harness-fragment-total"], fragmentDigest = current.headers["harness-fragment-sha256"];
                  if (typeof fragmentID !== "string" || !/^[0-9a-f-]{36}$/.test(fragmentID)
                      || typeof fragmentOffset !== "string" || !/^(0|[1-9][0-9]*)$/.test(fragmentOffset)
                      || typeof fragmentTotal !== "string" || !/^[1-9][0-9]*$/.test(fragmentTotal)
                      || typeof fragmentDigest !== "string" || !/^[0-9a-f]{64}$/.test(fragmentDigest)) throw new Error("Invalid fragment metadata");
                  const parsedOffset = Number(fragmentOffset), parsedTotal = Number(fragmentTotal);
                  if (!Number.isSafeInteger(parsedOffset) || !Number.isSafeInteger(parsedTotal) || !canonicalStream && parsedTotal > bufferConstants.MAX_STRING_LENGTH
                      || parsedOffset !== offset || offset === 0 && parsedTotal <= 0) throw new Error("Invalid fragment bounds");
                  if (!id) { id = fragmentID; cleanupID = id; total = parsedTotal; digest = fragmentDigest; }
                  else if (id !== fragmentID || total !== parsedTotal || digest !== fragmentDigest) throw new Error("Fragment identity changed");
                  let length = 0;
                  for await (const rawChunk of current) {
                    const chunk = Buffer.from(rawChunk);
                    length += chunk.length;
                    if (length > fragmentByteLimit || offset + length > total) throw new Error("Fragment exceeds budget");
                    hash.update(chunk); decode(chunk);
                  }
                  if (!current.complete || !length || offset + length > total) throw new Error("Incomplete fragment");
                  if (!firstLength) firstLength = length;
                  else if (length > firstLength || offset + length < total && length !== firstLength) throw new Error("Invalid fragment length");
                  offset += length;
                  if (offset === total) break;
                  // Each continuation is independently authenticated. Limit
                  // parallel buffering and consume bytes in offset order so
                  // UTF-8 and the digest still cover one immutable snapshot.
                  const fragmentTarget = new URL(this.endpoint); fragmentTarget.pathname = "/v2/response-fragment";
                  while (offset < total) {
                    const offsets: number[] = [];
                    for (let next = offset; next < total && offsets.length < 4; next += firstLength) offsets.push(next);
                    const batch = await Promise.all(offsets.map(async expectedOffset => {
                      const fragmentBody = canonicalJson({ id, offset: expectedOffset });
                      if (Buffer.byteLength(fragmentBody) > this.maxPayloadBytes) throw new Error("Fragment request exceeds payload limit");
                      const nextToken = await this.tokenProvider();
                      if (settled || typeof nextToken !== "string" || !/^[\x21-\x7e]+$/.test(nextToken)) throw new Error("Credential unavailable");
                      cleanupToken = nextToken;
                      const responsePart = await new Promise<import("node:http").IncomingMessage>((resolve, reject) => {
                        const pending = send(fragmentTarget, { method: "POST", agent: this.#agent, headers: { "content-type": "application/json; charset=utf-8",
                          authorization: `Bearer ${nextToken}`, "content-length": Buffer.byteLength(fragmentBody) } }, response => {
                            response.once("close", () => fragmentRequests.delete(pending)); resolve(response);
                          });
                        network = pending; fragmentRequests.add(pending);
                        pending.once("error", error => { fragmentRequests.delete(pending); reject(error); }); pending.end(fragmentBody);
                      });
                      if (settled || responsePart.statusCode !== 200 || responsePart.headers["harness-fragment-version"] !== "2"
                          || !/^application\/octet-stream(?:\s*;|$)/i.test(responsePart.headers["content-type"] ?? "")
                          || responsePart.headers["harness-fragment-id"] !== id
                          || responsePart.headers["harness-fragment-offset"] !== String(expectedOffset)
                          || responsePart.headers["harness-fragment-total"] !== String(total)
                          || responsePart.headers["harness-fragment-sha256"] !== digest) throw new Error("Invalid fragment identity");
                      const chunks: Buffer[] = []; let received = 0;
                      for await (const raw of responsePart) {
                        const chunk = Buffer.from(raw); received += chunk.length;
                        if (received > fragmentByteLimit || expectedOffset + received > total) throw new Error("Fragment exceeds budget");
                        chunks.push(chunk);
                      }
                      if (!responsePart.complete || received !== Math.min(firstLength, total - expectedOffset)) throw new Error("Incomplete fragment");
                      return Buffer.concat(chunks, received);
                    }));
                    if (settled) throw new Error("Fragment request cancelled");
                    for (const bytes of batch) { hash.update(bytes); decode(bytes); offset += bytes.length; }
                  }
                  break;
                }
                if (hash.digest("hex") !== digest) throw new Error("Fragment digest mismatch");
                const assembled = decoder instanceof InternalCanonicalJsonDecoder ? decoder.finish() : parseJson(parts.join("") + decoder.decode());
                const envelope = internal ? validateNodeResponse(assembled, method as NodeMethod) : validateRPCResponse(assembled, method as PublicMethod);
                // Parallel responses can finish out of order across a grant
                // change. Reauthorize the same frozen snapshot after assembly,
                // immediately before returning any value or error detail.
                const finalToken = await this.tokenProvider();
                if (settled || typeof finalToken !== "string" || !/^[\x21-\x7e]+$/.test(finalToken)) throw new Error("Credential unavailable");
                cleanupToken = finalToken;
                const confirmBody = canonicalJson({ id, offset: 0 });
                if (Buffer.byteLength(confirmBody) > this.maxPayloadBytes) throw new Error("Fragment confirmation exceeds payload limit");
                const confirmTarget = new URL(this.endpoint); confirmTarget.pathname = "/v2/response-fragment";
                const confirmation = await new Promise<import("node:http").IncomingMessage>((resolve, reject) => {
                  const pending = send(confirmTarget, { method: "POST", agent: this.#agent, headers: { "content-type": "application/json; charset=utf-8",
                    authorization: `Bearer ${finalToken}`, "content-length": Buffer.byteLength(confirmBody) } }, response => {
                    response.once("close", () => fragmentRequests.delete(pending)); resolve(response);
                  });
                  network = pending; fragmentRequests.add(pending);
                  pending.once("error", error => { fragmentRequests.delete(pending); reject(error); }); pending.end(confirmBody);
                });
                if (settled || confirmation.statusCode !== 200 || confirmation.headers["harness-fragment-version"] !== "2"
                    || !/^application\/octet-stream(?:\s*;|$)/i.test(confirmation.headers["content-type"] ?? "")
                    || confirmation.headers["harness-fragment-id"] !== id || confirmation.headers["harness-fragment-offset"] !== "0"
                    || confirmation.headers["harness-fragment-total"] !== String(total) || confirmation.headers["harness-fragment-sha256"] !== digest)
                  throw new Error("Fragment authorization confirmation failed");
                let confirmed = 0;
                for await (const chunk of confirmation) {
                  confirmed += Buffer.byteLength(chunk);
                  if (confirmed > fragmentByteLimit || confirmed > firstLength) throw new Error("Fragment confirmation exceeds budget");
                }
                if (!confirmation.complete || confirmed !== firstLength || settled) throw new Error("Incomplete fragment confirmation");
                if (!envelope.ok) {
                  if (internal || envelope.requestID !== requestID || binding && envelope.binding && !sameBinding(envelope.binding, binding)
                      || ["UNAUTHENTICATED", "FORBIDDEN", "PROTOCOL_MISMATCH"].includes(envelope.error.code)) throw new Error("Invalid fragmented rejection");
                  releaseSnapshot(); finish(new HarnessError(envelope.error)); return;
                }
                if (envelope.requestID !== requestID || !envelope.binding || binding && !sameBinding(envelope.binding, binding)) throw new Error("Unbound fragmented response");
                if (!internal && method === "session.getInfo" && !sameBinding(envelope.binding, envelope.result as DomainInfo)) throw new Error("Inconsistent fragmented handshake");
                if (!internal && method === "session.getInfo") {
                  this.supportsNodeProtocol = response.headers[NODE_PROTOCOL_HEADER] === NODE_PROTOCOL_CAPABILITY;
                  this.supportsStagedReports = response.headers["harness-report-transfer-version"] === "4";
                }
                if (internal) {
                  this.maxNodeReportBytes = Math.max(this.maxNodeReportBytes, Number(nodeRequestBudget));
                  this.maxNodeResponseBytes = Math.max(this.maxNodeResponseBytes, Number(nodeResponseBudget));
                  this.supportsNodeFragments = true;
                  if (reportFinalizeVersion === "3") this.supportsReportFinalize = true;
                }
                // Best-effort cleanup after integrity and binding are confirmed.
                // A lost cleanup request is bounded by server expiry.
                releaseSnapshot();
                finish(undefined, envelope.result);
              })().catch(() => { releaseSnapshot(); uncertain(); });
              return;
            }
            const chunks: string[] = [];
            const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
            let bytes = 0;
            let responseLimit = internal ? this.maxNodeResponseBytes : this.maxResponseBytes;
            const initialHandshake = !internal && method === "session.getInfo" && binding === null;
            const advertised = initialHandshake && response.statusCode === 200 ? response.headers["harness-max-response-bytes"] : undefined;
            const nodeClaim = internal && method === "claim" && response.statusCode === 200;
            const nodeRequestBudget = nodeClaim ? response.headers["harness-node-max-request-bytes"] : undefined;
            const nodeResponseBudget = nodeClaim ? response.headers["harness-node-max-response-bytes"] : undefined;
            const nodeFragmentVersion = nodeClaim ? response.headers["harness-node-fragment-version"] : undefined;
            const reportFinalizeVersion = nodeClaim ? response.headers["harness-node-report-finalize-version"] : undefined;
            const validBudget = (value: unknown): value is string => typeof value === "string" && /^[1-9][0-9]*$/.test(value)
              && Number.isSafeInteger(Number(value)) && Number(value) <= Number.MAX_SAFE_INTEGER;
            if (reportFinalizeVersion !== undefined && reportFinalizeVersion !== "3" || nodeFragmentVersion !== undefined && nodeFragmentVersion !== "2" || (nodeRequestBudget !== undefined || nodeResponseBudget !== undefined)
                && (!validBudget(nodeRequestBudget) || !validBudget(nodeResponseBudget))) {
              uncertain(); response.destroy(); return;
            }
            // The advertised budget is only usable after this response is
            // fully received and bound to the original claim. This first
            // packet remains subject to the previously negotiated budget.
            if (advertised !== undefined) {
              if (typeof advertised !== "string" || !/^[1-9][0-9]*$/.test(advertised) || !Number.isSafeInteger(Number(advertised))) {
                uncertain(); response.destroy(); return;
              }
              responseLimit = Number(advertised);
            }
            response.on("data", (chunk: Buffer) => {
              if (chunk.length > responseLimit - bytes) { uncertain(); response.destroy(); return; }
              bytes += chunk.length;
              try { chunks.push(decoder.decode(chunk, { stream: true })); }
              catch { uncertain(); response.destroy(); }
            });
            response.on("error", uncertain);
            response.on("aborted", uncertain);
            response.on("end", () => {
              if (settled) return;
              try {
                if (!response.complete || !/^application\/json(?:\s*;|$)/i.test(response.headers["content-type"] ?? "")) throw new Error("Invalid response");
                chunks.push(decoder.decode());
                const raw = chunks.join("");
                const value = parseJson(raw);
                const envelope = internal ? validateNodeResponse(value, method as NodeMethod) : validateRPCResponse(value, method as PublicMethod);
                if ((advertised !== undefined || nodeRequestBudget !== undefined || nodeFragmentVersion !== undefined) && !envelope.ok) throw new Error("Unexpected budget header");
                if (envelope.ok) {
                  if (envelope.requestID !== requestID || response.statusCode !== 200 || !envelope.binding || binding && !sameBinding(envelope.binding, binding)) throw new Error("Unbound response");
                  if (method === "session.getInfo") {
                    const info = envelope.result as DomainInfo;
                    if (!sameBinding(envelope.binding, info) || bytes > info.maxResponseBytes
                        || advertised !== undefined && info.maxResponseBytes !== responseLimit) throw new Error("Inconsistent handshake");
                    this.supportsNodeProtocol = response.headers[NODE_PROTOCOL_HEADER] === NODE_PROTOCOL_CAPABILITY;
                    this.supportsStagedReports = response.headers["harness-report-transfer-version"] === "4";
                  }
                  if (internal && method === "claim") {
                    const assignment = (envelope.result as { assignment: NodeAssignment | null }).assignment;
                    if (nodeRequestBudget !== undefined && nodeResponseBudget !== undefined) {
                      this.maxNodeReportBytes = Math.max(this.maxNodeReportBytes, Number(nodeRequestBudget));
                      this.maxNodeResponseBytes = Math.max(this.maxNodeResponseBytes, Number(nodeResponseBudget));
                      if (nodeFragmentVersion === "2") this.supportsNodeFragments = true;
                      if (reportFinalizeVersion === "3") this.supportsReportFinalize = true;
                    } else if (assignment && "action" in assignment && assignment.reportValueLimit !== undefined) {
                      this.maxNodeReportBytes = Math.max(this.maxNodeReportBytes,
                        nodeDataLimit(this.maxPayloadBytes, assignment.reportValueLimit, this.maxResponseBytes));
                    }
                  }
                  finish(undefined, envelope.result);
                } else {
                  const preparse = envelope.requestID === null && (
                    response.statusCode === 400 && envelope.error.code === "INVALID_ARGUMENT"
                    || response.statusCode === 401 && envelope.error.code === "UNAUTHENTICATED"
                    || response.statusCode === 413 && envelope.error.code === "LIMIT_EXCEEDED"
                    || response.statusCode === 426 && envelope.error.code === "PROTOCOL_MISMATCH");
                  const status = response.statusCode;
                  const validStatus = status === 200 && !["UNAUTHENTICATED", "FORBIDDEN", "PROTOCOL_MISMATCH"].includes(envelope.error.code)
                    || status === 400 && envelope.error.code === "INVALID_ARGUMENT"
                    || status === 401 && envelope.error.code === "UNAUTHENTICATED"
                    || status === 403 && envelope.error.code === "FORBIDDEN"
                    || status === 413 && envelope.error.code === "LIMIT_EXCEEDED"
                    || status === 426 && envelope.error.code === "PROTOCOL_MISMATCH";
                  if (!validStatus
                      || envelope.requestID !== requestID && !preparse
                      || binding && envelope.binding && !sameBinding(envelope.binding, binding) && envelope.error.code !== "SESSION_MISMATCH") throw new Error("Invalid rejection");
                  finish(new HarnessError(envelope.error));
                }
              } catch { uncertain(); }
            });
          };
          if (!nodeFragments) {
            network = send(target, {
              method: "POST", agent: this.#agent,
              headers: { "content-type": "application/json; charset=utf-8", authorization: `Bearer ${token}`, "content-length": Buffer.byteLength(body),
                ...(internal && (method === "register" || method === "claim") ? { [NODE_PROTOCOL_HEADER]: NODE_PROTOCOL_CAPABILITY } : {}),
                ...(checkpointHint ? { "harness-node-checkpoint-digest": checkpointHint } : {}),
                ...(internal && method === "report" && this.supportsNodeFragments ? { "harness-node-report-echo-version": "2" } : {}),
                ...(!internal ? { "harness-fragment-version": "2", "harness-fragment-limit": String(this.maxResponseBytes) }
                  : ["claim", "checkpointAction", "readCheckpoint", "readValidationValue"].includes(method) ? { "harness-node-response-fragments": "2", "harness-fragment-limit": String(this.maxNodeResponseBytes) } : {}) },
            }, onResponse);
            network.on("error", uncertain);
            sent = true; network.end(body);
          } else {
            const sender = upload!.sender;
            const fragmentTarget = new URL(this.endpoint); fragmentTarget.pathname = "/v2/node-request-fragment";
            let offset = upload!.offset;
            while (offset < sender.total) {
              const fragment = sender.fragment(offset), final = offset + fragment.bytes.length === sender.total;
              let received: import("node:http").IncomingMessage | undefined;
              for (let attempt = 0; attempt < 3 && !received; attempt++) {
                if (settled) return;
                try {
                  received = await new Promise<import("node:http").IncomingMessage>((resolve, reject) => {
                    network = send(fragmentTarget, { method: "POST", agent: this.#agent, headers: { "content-type": "application/octet-stream",
                      authorization: `Bearer ${token}`, "content-length": fragment.bytes.length, "harness-fragment-version": "2", "harness-node-canonical-request": "1", [NODE_PROTOCOL_HEADER]: NODE_PROTOCOL_CAPABILITY,
                      ...(checkpointHint ? { "harness-node-checkpoint-digest": checkpointHint } : {}),
                      "harness-transfer-id": fragment.meta.transferID, "harness-fragment-offset": String(fragment.meta.offset),
                      "harness-fragment-total": String(fragment.meta.total), "harness-fragment-sha256": fragment.meta.digest,
                      ...(finalizableReport && request.method === "reportAction" ? { "harness-node-report-finalize-version": "3",
                        "harness-node-report-expires-at": String(request.params.grant.expiresAt),
                        "harness-node-report-observation-only": request.params.observationOnly === true ? "1" : "0" } : {}),
                      ...(final && (method === "checkpointAction" || method === "readValidationValue") ? { "harness-node-response-fragments": "2", "harness-fragment-limit": String(this.maxNodeResponseBytes) } : {}) } }, resolve);
                    network.once("error", reject); sent = true; network.end(fragment.bytes);
                  });
                } catch { if (attempt === 2) throw new Error("Node fragment response lost"); }
              }
              if (!received) throw new Error("Node fragment response unavailable");
              if (received.statusCode === 400 && received.headers["harness-fragment-reset"] === "2" && offset > 0) {
                received.resume();
                if (settled) return;
                offset = 0; upload!.offset = 0; continue;
              }
              if (final) { onResponse(received); return; }
              if (received.statusCode !== 200 || !/^application\/json(?:\s*;|$)/i.test(received.headers["content-type"] ?? "")) throw new Error("Node fragment rejected");
              const chunks: Buffer[] = []; let bytes = 0;
              for await (const chunk of received) { const value = Buffer.from(chunk); bytes += value.length; if (bytes > 1024) throw new Error("Invalid node fragment acknowledgement"); chunks.push(value); }
              const ack = parseJson(Buffer.concat(chunks).toString("utf8"));
              if (!received.complete || !ack || typeof ack !== "object" || Array.isArray(ack) || ack.version !== 2
                  || ack.transferID !== sender.transferID || ack.nextOffset !== offset + fragment.bytes.length || Object.keys(ack).length !== 3) throw new Error("Invalid node fragment acknowledgement");
              offset += fragment.bytes.length;
              if (settled) return;
              upload!.offset = offset;
            }
          }
        } catch { uncertain(); }
      })();
    });
  }

  close(): void {
    this.#closed = true;
    for (const abort of this.#pending) abort();
    this.#uploads.clear();
    this.#agent.destroy();
  }
}

function sameBinding(left: SessionBinding, right: SessionBinding): boolean {
  return left.domainID === right.domainID && left.principalID === right.principalID;
}
function duration(value: unknown): void {
  if (value !== undefined && (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)) {
    throw harnessError("INVALID_ARGUMENT", "timeoutMs must be a nonnegative safe integer");
  }
}

async function beforeTimeout(promise: Promise<unknown>, timeoutMs: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = performance.now() + timeoutMs;
  try {
    return await Promise.race([promise.then(() => true), new Promise<false>(resolve => {
      const check = () => {
        const remaining = deadline - performance.now();
        if (remaining <= 0) resolve(false);
        else timer = setTimeout(check, Math.min(remaining, 2_147_483_647));
      };
      timer = setTimeout(check, Math.min(timeoutMs, 2_147_483_647));
    })]);
  } finally { if (timer) clearTimeout(timer); }
}
