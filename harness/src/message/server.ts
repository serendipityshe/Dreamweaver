import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { createHash } from "node:crypto";
import {
  canonicalJson, canonicalJsonChunks, canonicalJsonDigest, harnessError, HarnessError, parseJson, toHarnessError, validateErrorInfo, validateRPCRequest, validateRPCResponse,
  validateNodeRequest, validateNodeResponse, InternalCanonicalJsonDecoder,
  type DomainInfo, type RPCRequest, type NodeRPCRequest, type NodeRPCMap, type SessionBinding, type ErrorInfo, type ValidationPayload, type Json,
} from "../contract/index.js";
import { authorizeDefinition, authorizeNodeOperation, authorizeOperation, type PrincipalGrant, type StaticAuthentication } from "./auth.js";
import { carriesNodeData, nodeDataLimit } from "./limits.js";
import { ResponseFragments, type ResponseFragment } from "./response-fragments.js";
import { NodeFragmentReceiver } from "./node-fragments.js";
import { DiskNodeFragments } from "./disk-node-fragments.js";
import { checkpointDigest } from "./checkpoint-echo.js";
import { NODE_PROTOCOL_CAPABILITY, NODE_PROTOCOL_HEADER } from "./node-protocol-version.js";
import type { CanonicalReportStaging } from "./canonical-report-staging.js";
import type { BoundReportTransfer } from "./bound-report-transfer.js";
import { BoundErrorFragments, type BoundErrorFragment } from "./bound-error-fragments.js";

export type RPCHandler = (request: RPCRequest, principal: PrincipalGrant, revalidate?: () => PrincipalGrant,
  context?: { fragmentedResponses: boolean }) => unknown | Promise<unknown>;
export type NodeHandler = (request: NodeRPCRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant) => unknown | Promise<unknown>;
export interface RPCServerOptions {
  host?: string;
  port?: number;
  domainInfo: Omit<DomainInfo, "principalID">;
  /** Host-derived upper bound for reports from previously issued Attempts. */
  maxNodeValueBytes?: number;
  /** Existing host-owned parent of a per-instance disposable staging directory. */
  fragmentStagingParent?: string;
  /** Explicitly injected private staging seam; it does not publish reports. */
  reportStaging?: CanonicalReportStaging;
  /** Private, explicitly injected test seam; not an advertised node protocol. */
  boundReportTransfer?: BoundReportTransfer;
  authorizeStagedWorker?: (nodeID: string | undefined, incarnation: string | undefined, principal: PrincipalGrant) => void;
  /** Explicit test-only opt-in; production must not advertise until complete. */
  advertiseBoundReportTransfer?: boolean;
  authentication: StaticAuthentication;
  /** Bound before activation to reserve the port; unavailable until the host is ready. */
  isReady?: () => boolean;
  /** Host-injected coordinator gate; required for handshakes as well as queries. */
  assertAuthority(): void;
  handle: RPCHandler;
  /** Recheck the original result's authorization without re-executing its RPC. */
  authorizeFragment?: (request: RPCRequest, result: unknown, principal: PrincipalGrant, revalidate: () => PrincipalGrant) => void | Promise<void>;
  prepareFragmentAuthorization?: (request: RPCRequest, result: unknown, principal: PrincipalGrant, revalidate: () => PrincipalGrant) => unknown | Promise<unknown>;
  authorizeFragmentDescriptor?: (request: RPCRequest, descriptor: unknown, principal: PrincipalGrant, revalidate: () => PrincipalGrant) => void | Promise<void>;
  authorizeErrorFragment?: (request: RPCRequest, error: ErrorInfo, principal: PrincipalGrant, revalidate: () => PrincipalGrant) => void | Promise<void>;
  authorizeFragmentFinal?: (request: RPCRequest, result: unknown, principal: PrincipalGrant) => void;
  authorizeFragmentDescriptorFinal?: (request: RPCRequest, descriptor: unknown, principal: PrincipalGrant) => void;
  authorizeErrorFragmentFinal?: (request: RPCRequest, error: ErrorInfo, principal: PrincipalGrant) => void;
  authorizeNodeFragment?: (request: NodeRPCRequest, result: unknown, principal: PrincipalGrant, revalidate: () => PrincipalGrant) => void | Promise<void>;
  authorizeNodeFragmentFinal?: (request: NodeRPCRequest, result: unknown, principal: PrincipalGrant) => void;
  handleNode?: NodeHandler;
}
export interface RPCServer { endpoint: string; close(): Promise<void> }

function validationTarget(payload: ValidationPayload): { kind: "agent" | "capacity" | "policy"; ref: { id: string; version: string } } {
  if (payload.phase !== "rewrite") return payload;
  const context = payload.context;
  if (!context || typeof context !== "object" || Array.isArray(context) || !Array.isArray(context.policies)) throw harnessError("INVALID_ARGUMENT", "Invalid rewrite policy binding");
  const policy = context.policies[0];
  if (!policy || typeof policy !== "object" || Array.isArray(policy) || !policy.ref || typeof policy.ref !== "object" || Array.isArray(policy.ref)
      || typeof policy.ref.id !== "string" || typeof policy.ref.version !== "string") throw harnessError("INVALID_ARGUMENT", "Invalid rewrite policy binding");
  return { kind: "policy", ref: { id: policy.ref.id, version: policy.ref.version } };
}

/** Public transport only: business queries and mutations are injected by assembly. */
export async function startRPCServer(options: RPCServerOptions): Promise<RPCServer> {
  const info = copyDomainInfo(options.domainInfo);
  const maxNodeValueBytes = options.maxNodeValueBytes ?? info.maxValueBytes;
  if (!Number.isSafeInteger(maxNodeValueBytes) || maxNodeValueBytes < info.maxValueBytes) throw harnessError("INVALID_ARGUMENT", "Invalid historical node value budget");
  const maxNodeReportBytes = nodeDataLimit(info.maxPayloadBytes, maxNodeValueBytes, info.maxResponseBytes);
  // Private node frames have a separately negotiated finite budget. Public
  // RPC envelopes continue to use the published maxResponseBytes.
  const maxNodeResponseBytes = Math.max(info.maxResponseBytes, 1_048_576);
  const port = options.port ?? 0;
  if (!Number.isSafeInteger(port) || port < 0 || port > 65535 || typeof options.handle !== "function" || typeof options.assertAuthority !== "function"
      || options.handleNode !== undefined && typeof options.handleNode !== "function"
      || options.isReady !== undefined && typeof options.isReady !== "function"
      || options.boundReportTransfer && (!options.reportStaging || typeof options.authorizeStagedWorker !== "function")
      || options.advertiseBoundReportTransfer !== undefined && typeof options.advertiseBoundReportTransfer !== "boolean"
      || options.advertiseBoundReportTransfer === true && !options.boundReportTransfer) {
    throw harnessError("INVALID_ARGUMENT", "Invalid RPC server configuration");
  }
  const server = createServer((request, response) => {
    // Startup is transport unavailability, not proof that a caller lost its lease.
    // Refuse before reading/staging a body; normal authority checks remain intact.
    if (options.isReady && !options.isReady()) { response.destroy(); return; }
    void handle(request, response);
  });
  const fragments = new ResponseFragments();
  const boundErrors = new BoundErrorFragments();
  const nodeFragments = options.fragmentStagingParent
    ? await DiskNodeFragments.create(options.fragmentStagingParent, 1_073_741_824)
    : new NodeFragmentReceiver({ maxFrameBytes: 1_048_576, maxTransferBytes: 134_217_728,
      maxActiveTransfers: 64, maxReservedBytes: 268_435_456, ttlMs: 120_000 });
  const nodeFlights = new Map<string, Promise<void>>();
  let nodeFlightWaiters = 0;
  let reportBodies = 0;
  server.requestTimeout = 30_000;
  server.headersTimeout = 10_000;
  // Leave room for Node's default 1s client safety buffer so sockets remain reusable.
  server.keepAliveTimeout = 5_000;

  async function handle(incoming: IncomingMessage, outgoing: ServerResponse): Promise<void> {
    let requestID: string | null = null;
    let binding: SessionBinding | null = null;
    let parsed = false;
    let possiblyCommitted = false;
    let releaseFlight: (() => void) | undefined;
    let releaseReportBody: (() => void) | undefined;
    let publicRequest: RPCRequest | undefined;
    let publicFragmentLimit: number | undefined;
    let boundFailure: { owner: { domainID: string; principalID: string; nodeID: string }; transferID: string; control: boolean } | undefined;
    let token = "";
    const checkNodeFragment = async (request: NodeRPCRequest, result: unknown, principalID: string): Promise<void> => {
      if (!options.authorizeNodeFragment) throw harnessError("FORBIDDEN", "Node response fragment authorization is unavailable");
      const revalidate = () => {
        options.assertAuthority();
        const current = options.authentication.authenticate(token);
        if (current.principalID !== principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before the response");
        authorizeNodeOperation(current, request.method, request.params.nodeID);
        return current;
      };
      await options.authorizeNodeFragment(request, result, revalidate(), revalidate);
      const current = revalidate();
      options.authorizeNodeFragmentFinal?.(request, result, current);
      options.assertAuthority();
    };
    const publicPrincipal = (request: RPCRequest, principalID: string): PrincipalGrant => {
      options.assertAuthority();
      const current = options.authentication.authenticate(token);
      if (current.principalID !== principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before the response");
      authorizeOperation(current, request.method);
      return current;
    };
    const checkPublicFragment = async (request: RPCRequest, value: unknown, principalID: string, kind: "result" | "descriptor" | "error"): Promise<void> => {
      const authorize = kind === "result" ? options.authorizeFragment : kind === "descriptor" ? options.authorizeFragmentDescriptor : options.authorizeErrorFragment;
      const final = kind === "result" ? options.authorizeFragmentFinal : kind === "descriptor" ? options.authorizeFragmentDescriptorFinal : options.authorizeErrorFragmentFinal;
      if (!authorize) throw harnessError("FORBIDDEN", "Response fragment authorization is unavailable");
      const revalidate = () => publicPrincipal(request, principalID);
      const outcome = (authorize as (request: RPCRequest, value: unknown, principal: PrincipalGrant, revalidate: () => PrincipalGrant) => void | Promise<void>)(request, value, revalidate(), revalidate);
      const asynchronous = !!outcome && typeof (outcome as Promise<void>).then === "function";
      await outcome;
      const fresh = revalidate();
      if (final) (final as (request: RPCRequest, value: unknown, principal: PrincipalGrant) => void)(request, value, fresh);
      else if (!asynchronous) (authorize as (request: RPCRequest, value: unknown, principal: PrincipalGrant, revalidate: () => PrincipalGrant) => void)(request, value, fresh, revalidate);
      else throw harnessError("FORBIDDEN", "Final response authorization is unavailable");
      options.assertAuthority();
    };
    try {
      token = credential(incoming);
      options.authentication.authenticate(token);
      if (incoming.method !== "POST" || !["/v1/rpc", "/v1/node", "/v2/response-fragment", "/v2/node-request-fragment", ...(options.reportStaging ? ["/v4/report-fragment"] : []),
        ...(options.boundReportTransfer ? ["/v4/report-control-fragment", "/v4/bound-report-fragment", "/v4/error-fragment"] : [])].includes(incoming.url ?? "")) {
        throw harnessError("INVALID_ARGUMENT", "Expected a Harness POST endpoint");
      }
      const nodeFragment = incoming.url === "/v2/node-request-fragment";
      const stagedReportFragment = incoming.url === "/v4/report-fragment" && !!options.reportStaging;
      const controlFragment = incoming.url === "/v4/report-control-fragment" && !!options.boundReportTransfer;
      const boundReportFragment = incoming.url === "/v4/bound-report-fragment" && !!options.boundReportTransfer;
      if (!(nodeFragment || stagedReportFragment || controlFragment || boundReportFragment ? /^application\/octet-stream\s*$/i.test(incoming.headers["content-type"] ?? "")
        : /^application\/json(?:\s*;\s*charset\s*=\s*utf-8)?\s*$/i.test(incoming.headers["content-type"] ?? ""))
          || incoming.headers["content-encoding"] && incoming.headers["content-encoding"] !== "identity") {
        throw harnessError("INVALID_ARGUMENT", "Expected UTF-8 application/json");
      }
      // Bound the aggregate of frame buffers before reading request bodies.
      // Do not queue unlimited sockets while waiting for decode admission.
      if (stagedReportFragment || controlFragment || boundReportFragment) {
        const gate = options.reportStaging!.concurrency;
        if (reportBodies >= gate.maxConcurrent + gate.maxPending) {
          const current = options.authentication.authenticate(token);
          options.assertAuthority();
          const transferID = incoming.headers["harness-transfer-id"], nodeHash = incoming.headers["harness-node-hash"];
          if ((controlFragment || boundReportFragment) && current.roles.includes("worker") && typeof transferID === "string"
              && /^[A-Za-z0-9_-]{1,128}$/.test(transferID) && typeof nodeHash === "string" && /^[a-f0-9]{64}$/.test(nodeHash)) {
            options.authorizeStagedWorker!(undefined, undefined, current);
            const hash = createHash("sha256");
            for (const part of canonicalJsonChunks([info.domainID, current.principalID, nodeHash])) hash.update(part);
            const encoded = canonicalJson({ version: 4, transferID, workerDigest: hash.digest("hex"), ok: false,
              // This frame was not read, but an earlier attempt with the
              // same identity may already have committed before its ACK was lost.
              error: harnessError("LIMIT_EXCEEDED", "Report transfer is busy", null, "uncertain").info });
            if (Buffer.byteLength(encoded) <= info.maxResponseBytes) {
              outgoing.writeHead(429, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": Buffer.byteLength(encoded) });
              outgoing.end(encoded); incoming.resume(); return;
            }
          }
          throw harnessError("LIMIT_EXCEEDED", "Report transfer is busy");
        }
        reportBodies++;
        releaseReportBody = () => { reportBodies--; };
      }
      const body = await readBody(incoming, stagedReportFragment || controlFragment || boundReportFragment ? 65_536 : nodeFragment ? 1_048_576 : incoming.url === "/v1/node" ? maxNodeReportBytes : info.maxPayloadBytes);
      if (incoming.url === "/v4/error-fragment") {
        let value: Json;
        try { value = parseJson(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(body)); }
        catch { throw harnessError("INVALID_ARGUMENT", "Invalid error fragment request"); }
        if (!value || typeof value !== "object" || Array.isArray(value) || typeof value.id !== "string"
            || !(Object.keys(value).sort().join(",") === "id,offset" && typeof value.offset === "number" && Number.isSafeInteger(value.offset)
              || Object.keys(value).sort().join(",") === "id,release" && value.release === true)) throw harnessError("INVALID_ARGUMENT", "Invalid error fragment request");
        const entry = boundErrors.get(value.id), current = options.authentication.authenticate(token);
        if (!entry || entry.owner.domainID !== info.domainID || entry.owner.principalID !== current.principalID)
          throw harnessError("FORBIDDEN", "Error fragment is unavailable");
        if (value.release === true) { boundErrors.release(value.id); outgoing.writeHead(204, { "cache-control": "no-store" }); outgoing.end(); return; }
        try { await authorizeBoundResponse(entry.owner, entry.transferID, token); }
        catch (error) { boundErrors.release(value.id); throw error; }
        sendBoundErrorFragment(outgoing, boundErrors.read(value.id, entry, value.offset as number)); return;
      }
      if (controlFragment || boundReportFragment) {
        const transferID = incoming.headers["harness-transfer-id"], rawOffset = incoming.headers["harness-fragment-offset"], rawTotal = incoming.headers["harness-fragment-total"],
          digest = incoming.headers["harness-fragment-sha256"], nodeHandle = incoming.headers["harness-node-hash"];
        if (incoming.headers["harness-fragment-version"] !== "4" || typeof transferID !== "string" || typeof nodeHandle !== "string" || !/^[a-f0-9]{64}$/.test(nodeHandle)
            || typeof rawOffset !== "string" || !/^(0|[1-9][0-9]*)$/.test(rawOffset) || typeof rawTotal !== "string" || !/^[1-9][0-9]*$/.test(rawTotal)
            || typeof digest !== "string") throw harnessError("INVALID_ARGUMENT", "Invalid bound report fragment metadata");
        requestID = transferID;
        options.assertAuthority();
        const principal = options.authentication.authenticate(token);
        binding = { domainID: info.domainID, principalID: principal.principalID };
        const owner = { domainID: info.domainID, principalID: principal.principalID, nodeID: nodeHandle };
        boundFailure = { owner, transferID, control: controlFragment };
        const assertWorker = (nodeID?: string, incarnation?: string) => {
          options.assertAuthority();
          const current = options.authentication.authenticate(token);
          if (current.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Report fragment principal changed");
          if (!current.roles.includes("worker")) throw harnessError("FORBIDDEN", "Report fragment requires a worker credential");
          if (nodeID !== undefined) authorizeNodeOperation(current, "reportAction", nodeID);
          options.authorizeStagedWorker!(nodeID, incarnation, current);
        };
        assertWorker();
        const meta = { transferID, offset: Number(rawOffset), total: Number(rawTotal), digest };
        let ack: unknown;
        if (controlFragment) {
          const received = await options.reportStaging!.accept(owner, meta, body);
          if (received.complete) {
            await options.boundReportTransfer!.open(owner, transferID, received.publication, assertWorker);
            ack = { version: 4, transferID, nextOffset: meta.total, bound: true };
          } else ack = { version: 4, transferID, nextOffset: received.nextOffset, bound: false };
        } else {
          const rawExpiry = incoming.headers["harness-node-report-expires-at"], observation = incoming.headers["harness-node-report-observation-only"];
          const isFinal = meta.offset + body.length === meta.total;
          if (isFinal && (typeof rawExpiry !== "string" || !/^[1-9][0-9]*$/.test(rawExpiry) || !Number.isSafeInteger(Number(rawExpiry))
              || observation !== "0" && observation !== "1")) throw harnessError("INVALID_ARGUMENT", "Invalid report finalization metadata");
          let result: Awaited<ReturnType<BoundReportTransfer["accept"]>>;
          try {
            result = await options.boundReportTransfer!.accept(owner, meta, body,
              isFinal ? { expiresAt: Number(rawExpiry), observationOnly: observation === "1" } : undefined, assertWorker);
          } catch (error) {
            if (isFinal && !(error instanceof HarnessError && error.info.disposition === "rejected")) possiblyCommitted = true;
            throw error;
          }
          if (result.complete) possiblyCommitted = true;
          ack = result.complete && !result.stored
            ? { version: 4, transferID, nextOffset: meta.total, stored: false, diagnostic: true }
            : { version: 4, transferID, nextOffset: result.complete ? meta.total : result.nextOffset, stored: result.complete };
        }
        // Recheck the complete source after every bound operation, including
        // after publication. A role-only check cannot detect node or fixed
        // definition permissions revoked across an await in the handler.
        if (boundReportFragment || controlFragment && (ack as { bound?: boolean }).bound === true)
          await options.boundReportTransfer!.authorizeResponse(owner, transferID, assertWorker);
        assertWorker();
        const encoded = canonicalJson(ack);
        outgoing.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": Buffer.byteLength(encoded) });
        outgoing.end(encoded); return;
      }
      if (stagedReportFragment) {
        const transferID = incoming.headers["harness-transfer-id"], rawOffset = incoming.headers["harness-fragment-offset"],
          rawTotal = incoming.headers["harness-fragment-total"], digest = incoming.headers["harness-fragment-sha256"], nodeID = incoming.headers["harness-node-id"];
        if (incoming.headers["harness-fragment-version"] !== "4" || typeof transferID !== "string" || typeof nodeID !== "string"
            || typeof rawOffset !== "string" || !/^(0|[1-9][0-9]*)$/.test(rawOffset)
            || typeof rawTotal !== "string" || !/^[1-9][0-9]*$/.test(rawTotal) || typeof digest !== "string")
          throw harnessError("INVALID_ARGUMENT", "Invalid canonical report fragment metadata");
        options.assertAuthority();
        const principal = options.authentication.authenticate(token);
        authorizeNodeOperation(principal, "reportAction", nodeID);
        const result = await options.reportStaging!.accept({ domainID: info.domainID, principalID: principal.principalID, nodeID },
          { transferID, offset: Number(rawOffset), total: Number(rawTotal), digest }, body);
        options.assertAuthority();
        const current = options.authentication.authenticate(token);
        if (current.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Report fragment principal changed");
        authorizeNodeOperation(current, "reportAction", nodeID);
        const ack = canonicalJson({ version: 4, transferID, nextOffset: result.complete ? Number(rawTotal) : result.nextOffset, sealed: result.complete });
        outgoing.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": Buffer.byteLength(ack) });
        outgoing.end(ack); return;
      }
      let fullBody = body;
      let canonicalValue: Json | undefined;
      const reportFinalize = nodeFragment && incoming.headers["harness-node-report-finalize-version"] === "3";
      if (nodeFragment) {
        if (incoming.headers["harness-fragment-version"] !== "2") throw harnessError("PROTOCOL_MISMATCH", "Unsupported node fragment protocol");
        const transferID = incoming.headers["harness-transfer-id"], rawOffset = incoming.headers["harness-fragment-offset"],
          rawTotal = incoming.headers["harness-fragment-total"], digest = incoming.headers["harness-fragment-sha256"];
        if (typeof transferID !== "string" || typeof rawOffset !== "string" || !/^(0|[1-9][0-9]*)$/.test(rawOffset)
            || typeof rawTotal !== "string" || !/^[1-9][0-9]*$/.test(rawTotal) || typeof digest !== "string") throw harnessError("INVALID_ARGUMENT", "Invalid node fragment metadata");
        options.assertAuthority();
        const current = options.authentication.authenticate(token);
        if (!current.roles.includes("worker")) throw harnessError("FORBIDDEN", "Node fragments require a worker credential");
        const canonical = incoming.headers["harness-node-canonical-request"] === "1";
        if (incoming.headers["harness-node-canonical-request"] !== undefined && !canonical) throw harnessError("PROTOCOL_MISMATCH", "Unsupported canonical node request encoding");
        if (canonical && incoming.headers[NODE_PROTOCOL_HEADER] !== NODE_PROTOCOL_CAPABILITY) throw harnessError("PROTOCOL_MISMATCH", "Canonical node request capability is incompatible");
        const owner = { domainID: `${reportFinalize ? `${info.domainID}:report-finalize-v3` : info.domainID}${canonical ? ":canonical-v1" : ""}`, principalID: current.principalID };
        // Serialize before reading a completed staged body: concurrent replays
        // must not each materialize an additional full-size Buffer.
        const flightKey = canonicalJson([owner.domainID, owner.principalID, transferID]);
        if (nodeFlightWaiters >= 64) throw harnessError("LIMIT_EXCEEDED", "Node fragment staging is busy");
        nodeFlightWaiters++;
        try { while (nodeFlights.has(flightKey)) await nodeFlights.get(flightKey); }
        finally { nodeFlightWaiters--; }
        let release!: () => void;
        nodeFlights.set(flightKey, new Promise<void>(resolve => { release = resolve; }));
        releaseFlight = () => { nodeFlights.delete(flightKey); release(); };
        options.assertAuthority();
        const refreshed = options.authentication.authenticate(token);
        if (refreshed.principalID !== owner.principalID || !refreshed.roles.includes("worker"))
          throw harnessError("FORBIDDEN", "Node fragment authorization changed");
        const assertCurrent = () => { options.assertAuthority(); const fresh = options.authentication.authenticate(token); if (fresh.principalID !== owner.principalID || !fresh.roles.includes("worker")) throw harnessError("FORBIDDEN", "Node fragment authorization changed"); };
        let received;
        try { received = canonical && nodeFragments instanceof DiskNodeFragments
          ? await nodeFragments.accept(owner, { transferID, offset: Number(rawOffset), total: Number(rawTotal), digest }, body, true, assertCurrent)
          : await nodeFragments.accept(owner, { transferID, offset: Number(rawOffset), total: Number(rawTotal), digest }, body); }
        catch (error) {
          if (error instanceof HarnessError && error.info.code === "INVALID_ARGUMENT" && error.info.message === "Node transfer must begin at offset zero")
            outgoing.setHeader("harness-fragment-reset", "2");
          throw error;
        }
        if (!received.complete) {
          const ack = canonicalJson({ version: 2, transferID, nextOffset: received.nextOffset });
          outgoing.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": Buffer.byteLength(ack) });
          outgoing.end(ack); return;
        }
        if (reportFinalize && Number(rawOffset) + body.length < Number(rawTotal)) {
          const ack = canonicalJson({ version: 2, transferID, nextOffset: Number(rawOffset) + body.length });
          outgoing.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": Buffer.byteLength(ack) });
          outgoing.end(ack); return;
        }
        assertCurrent();
        if ("value" in received) canonicalValue = received.value;
        else if (canonical) {
          const decoder = new InternalCanonicalJsonDecoder();
          for (let offset = 0; offset < received.bytes.length; offset += 65_536) { decoder.write(received.bytes.subarray(offset, offset + 65_536)); await new Promise<void>(resolve => setImmediate(resolve)); assertCurrent(); }
          canonicalValue = decoder.finish();
          let offset = 0; for (const expected of canonicalJsonChunks(canonicalValue)) { if (!received.bytes.subarray(offset, offset + expected.length).equals(Buffer.from(expected))) throw harnessError("INVALID_ARGUMENT", "Noncanonical node request encoding"); offset += expected.length; await new Promise<void>(resolve => setImmediate(resolve)); assertCurrent(); }
          if (offset !== received.bytes.length) throw harnessError("INVALID_ARGUMENT", "Canonical node request length changed");
        } else fullBody = received.bytes;
        incoming.url = "/v1/node";
      }
      let source: string;
      try { source = canonicalValue === undefined ? new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(fullBody) : ""; }
      catch { throw harnessError("INVALID_ARGUMENT", "Expected valid UTF-8 JSON"); }
      const value = canonicalValue === undefined ? parseJson(source) : canonicalValue;
      if (incoming.url === "/v2/response-fragment") {
        if (!value || typeof value !== "object" || Array.isArray(value) || typeof value.id !== "string"
            || !(Object.keys(value).sort().join(",") === "id,offset" && typeof value.offset === "number" && Number.isSafeInteger(value.offset)
              || Object.keys(value).sort().join(",") === "id,release" && value.release === true)) {
          throw harnessError("INVALID_ARGUMENT", "Invalid response fragment request");
        }
        options.assertAuthority();
        const entry = fragments.get(value.id);
        const current = options.authentication.authenticate(token);
        if (!entry || entry.binding.domainID !== info.domainID || entry.binding.principalID !== current.principalID) {
          throw harnessError("FORBIDDEN", "Response fragment is unavailable");
        }
        if (value.release === true) { fragments.release(value.id); outgoing.writeHead(204, { "cache-control": "no-store" }); outgoing.end(); return; }
        try {
          if (entry.node) {
            const original = entry.request as NodeRPCRequest;
            authorizeNodeOperation(current, original.method, original.params.nodeID);
            if (!options.authorizeNodeFragment) throw harnessError("FORBIDDEN", "Node response fragment authorization is unavailable");
            await checkNodeFragment(original, entry.result, entry.binding.principalID);
          } else {
            const original = entry.request as RPCRequest;
            authorizeOperation(current, original.method);
            if (original.method !== "session.getInfo") {
              if (entry.error) {
                if (!options.authorizeErrorFragment) throw harnessError("FORBIDDEN", "Error fragment authorization is unavailable");
                await checkPublicFragment(original, entry.result, entry.binding.principalID, "error");
              } else {
                if (!options.authorizeFragment) throw harnessError("FORBIDDEN", "Response fragment authorization is unavailable");
                if (entry.authorization !== undefined && options.authorizeFragmentDescriptor)
                  await checkPublicFragment(original, entry.authorization, entry.binding.principalID, "descriptor");
                else await checkPublicFragment(original, entry.result, entry.binding.principalID, "result");
              }
            }
          }
          options.assertAuthority();
        } catch (error) { fragments.release(value.id); throw error; }
        let fragment: ResponseFragment;
        try { fragment = fragments.read(value.id, entry, value.offset as number); }
        catch (error) { fragments.release(value.id); throw error; }
        sendFragment(outgoing, fragment);
        return;
      }
      if (value && typeof value === "object" && !Array.isArray(value) && typeof value.requestID === "string"
          && value.requestID.length > 0 && value.requestID.trim() === value.requestID) requestID = value.requestID;
      if (incoming.url === "/v1/node") {
        let request = validateNodeRequest(value);
        if (reportFinalize) {
          const rawExpiry = incoming.headers["harness-node-report-expires-at"], observation = incoming.headers["harness-node-report-observation-only"];
          if (request.method !== "reportAction" || typeof rawExpiry !== "string" || !/^[1-9][0-9]*$/.test(rawExpiry)
              || !Number.isSafeInteger(Number(rawExpiry)) || observation !== "0" && observation !== "1"
              || request.params.observationOnly === true && observation !== "1") throw harnessError("INVALID_ARGUMENT", "Invalid report finalization metadata");
          request = validateNodeRequest({ ...request, params: { ...request.params, grant: { ...request.params.grant, expiresAt: Number(rawExpiry) }, observationOnly: observation === "1" } });
        }
        if (nodeFragment && !carriesNodeData(request.method)) throw harnessError("LIMIT_EXCEEDED", "Node method does not accept fragmented data");
        const wantsNodeResponseFragments = incoming.headers["harness-node-response-fragments"] === "2";
        const nodeResponseLimit = incoming.headers["harness-fragment-limit"];
        if (wantsNodeResponseFragments && (typeof nodeResponseLimit !== "string" || !/^[1-9][0-9]*$/.test(nodeResponseLimit)
            || !Number.isSafeInteger(Number(nodeResponseLimit)))) throw harnessError("INVALID_ARGUMENT", "Invalid node response fragment budget");
        if (!nodeFragment && body.length > info.maxPayloadBytes && !carriesNodeData(request.method)) throw harnessError("LIMIT_EXCEEDED", "RPC request exceeds the published payload limit");
        parsed = true;
        options.assertAuthority();
        const principal = options.authentication.authenticate(token);
        binding = { domainID: info.domainID, principalID: principal.principalID };
        if (request.binding.domainID !== binding.domainID || request.binding.principalID !== binding.principalID) {
          throw harnessError("SESSION_MISMATCH", "Session binding does not match the current domain and authenticated principal");
        }
        authorizeNodeOperation(principal, request.method, request.params.nodeID);
        // Compatibility is checked before registration or claim can mutate the
        // session, including claims from sessions created by an older server.
        if ((request.method === "register" || request.method === "claim")
            && incoming.headers[NODE_PROTOCOL_HEADER] !== NODE_PROTOCOL_CAPABILITY) {
          throw harnessError("PROTOCOL_MISMATCH", "Node protocol capability is incompatible");
        }
        if (!(request.method === "readValidationValue" && wantsNodeResponseFragments && options.authorizeNodeFragment))
          assertEnvelopeCapacity(requestID!, binding, info.maxResponseBytes);
        let echo = request.method === "report" && (nodeFragment || incoming.headers["harness-node-report-echo-version"] === "2") ? { protocolVersion: "harness/1", requestID, binding, ok: true,
          result: { digest: canonicalJsonDigest(request.params.conclusion) } } : null;
        if (echo && Buffer.byteLength(canonicalJson(echo), "utf8") > info.maxResponseBytes) {
          if (nodeFragment) throw harnessError("LIMIT_EXCEEDED", "Node acknowledgement exceeds the published response limit");
          echo = null;
        }
        if (request.method === "register") {
          for (const definition of request.params.definitions) authorizeDefinition(principal, definition.kind, definition.ref.id, definition.ref.version);
        } else if (request.method === "report" || request.method === "verifyValidation" || request.method === "readValidationValue") {
          const assignment = request.params.assignment;
          const target = "action" in assignment ? { kind: "capacity" as const, ref: assignment.request.target } : "start" in assignment || "wake" in assignment ? { kind: "agent" as const, ref: assignment.agent } : "driver" in assignment || "finalize" in assignment ? { kind: "agent" as const, ref: assignment.input.run.agent } : validationTarget(assignment.payload);
          authorizeDefinition(principal, target.kind, target.ref.id, target.ref.version);
        }
        const revalidate = (): PrincipalGrant => {
          options.assertAuthority();
          const current = options.authentication.authenticate(token);
          if (request.binding.domainID !== info.domainID || request.binding.principalID !== current.principalID) {
            throw harnessError("SESSION_MISMATCH", "Session binding does not match the current domain and authenticated principal");
          }
          authorizeNodeOperation(current, request.method, request.params.nodeID);
          if (request.method === "register") {
            for (const definition of request.params.definitions) authorizeDefinition(current, definition.kind, definition.ref.id, definition.ref.version);
          } else if (request.method === "report" || request.method === "verifyValidation" || request.method === "readValidationValue") {
            const assignment = request.params.assignment;
            const target = "action" in assignment ? { kind: "capacity" as const, ref: assignment.request.target } : "start" in assignment || "wake" in assignment ? { kind: "agent" as const, ref: assignment.agent } : "driver" in assignment || "finalize" in assignment ? { kind: "agent" as const, ref: assignment.input.run.agent } : validationTarget(assignment.payload);
            authorizeDefinition(current, target.kind, target.ref.id, target.ref.version);
          }
          return current;
        };
        if (!options.handleNode) throw harnessError("STATE_CONFLICT", "Node service is unavailable");
        possiblyCommitted = true;
        let result: unknown;
        try { result = await options.handleNode(request, principal, revalidate); }
        catch (error) {
          if (error instanceof HarnessError) { validateErrorInfo(error.info); possiblyCommitted = false; }
          throw error;
        }
        if (request.method === "readValidationValue") {
          // Its immutable byte read can be shortened without changing the
          // assignment. Account for the actual binding and request identity,
          // rather than assuming a fixed envelope overhead.
          const part = result as NodeRPCMap["readValidationValue"]["result"];
          if (!part || typeof part.data !== "string") throw harnessError("INTERNAL_ERROR", "Invalid validation value response");
          const bytes = Buffer.from(part.data, "base64");
          if (!bytes.length || bytes.toString("base64") !== part.data) throw harnessError("INTERNAL_ERROR", "Invalid validation value bytes");
          const envelopeSize = (count: number) => Buffer.byteLength(canonicalJson({ protocolVersion: "harness/1", requestID, binding, ok: true,
            result: { offset: part.offset, data: bytes.subarray(0, count).toString("base64"), done: count === bytes.length && part.done } }), "utf8");
          if (envelopeSize(bytes.length) > info.maxResponseBytes) {
            let low = 0, high = bytes.length;
            while (low < high) {
              const middle = Math.ceil((low + high) / 2);
              if (envelopeSize(middle) <= info.maxResponseBytes) low = middle; else high = middle - 1;
            }
            if (!low) {
              if (!wantsNodeResponseFragments || !options.authorizeNodeFragment) {
                possiblyCommitted = false; throw harnessError("LIMIT_EXCEEDED", "Validation value response envelope exceeds the published limit");
              }
            } else result = { offset: part.offset, data: bytes.subarray(0, low).toString("base64"), done: false };
          }
        }
        // Internal methods mutate persistent sessions, assignments or receipts.
        // Preserve their real receipt/uncertainty across a subsequent takeover.
        let envelope;
        try { envelope = validateNodeResponse({ protocolVersion: "harness/1", requestID, binding, ok: true, result }, request.method); }
        catch { throw harnessError("INTERNAL_ERROR", "The operation produced an invalid node response"); }
        const checkpointHint = incoming.headers["harness-node-checkpoint-digest"];
        if ((request.method === "checkpointAction" || request.method === "readCheckpoint") && envelope.ok
            && typeof checkpointHint === "string" && /^[0-9a-f]{64}$/.test(checkpointHint) && wantsNodeResponseFragments && options.authorizeNodeFragment) {
          const checkpoint = (envelope.result as NodeRPCMap["readCheckpoint"]["result"]).checkpoint;
          if (checkpoint && checkpointDigest(requestID!, binding, request.params.operationID, checkpoint) === checkpointHint) {
            const acknowledgement = { protocolVersion: "harness/1", requestID, binding, ok: true,
              result: { operationID: request.params.operationID, digest: checkpointHint } };
            if (Buffer.byteLength(canonicalJson(acknowledgement), "utf8") <= Math.min(info.maxResponseBytes, Number(nodeResponseLimit))) {
              options.assertAuthority();
              const current = options.authentication.authenticate(token);
              if (current.principalID !== binding.principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before checkpoint confirmation");
              authorizeNodeOperation(current, request.method, request.params.nodeID);
              await checkNodeFragment(request, envelope.result, binding.principalID);
              send(outgoing, 200, acknowledgement, info.maxResponseBytes, false, { "harness-node-checkpoint-echo-version": "2" });
              return;
            }
          }
        }
        if (echo && request.method === "report") {
          if (canonicalJsonDigest(result) !== canonicalJsonDigest({ conclusion: request.params.conclusion })) throw new Error("Node report receipt is inconsistent");
          send(outgoing, 200, echo, info.maxResponseBytes, false, { "harness-node-echo-version": "2" });
          return;
        }
        const nodeBudgetHeaders = { "harness-node-max-request-bytes": String(maxNodeReportBytes),
          "harness-node-max-response-bytes": String(maxNodeResponseBytes), "harness-node-fragment-version": "2", "harness-node-report-finalize-version": "3" };
        let nodeSendLimit = info.maxResponseBytes;
        if (["claim", "checkpointAction", "readCheckpoint", "readValidationValue"].includes(request.method) && wantsNodeResponseFragments && options.authorizeNodeFragment && envelope.ok) {
          const snapshot = await measureResponseChunks(envelope), fragmentLimit = Math.min(maxNodeResponseBytes, Number(nodeResponseLimit), 1_048_576);
          nodeSendLimit = fragmentLimit;
          if (snapshot.total <= fragmentLimit) {
            options.assertAuthority();
            const afterMeasure = options.authentication.authenticate(token);
            if (afterMeasure.principalID !== binding.principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before the response");
            authorizeNodeOperation(afterMeasure, request.method, request.params.nodeID);
            await checkNodeFragment(request, envelope.result, binding.principalID);
          }
          if (snapshot.total > fragmentLimit) {
            options.assertAuthority();
            const current = options.authentication.authenticate(token);
            if (current.principalID !== binding.principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before the response");
            authorizeNodeOperation(current, request.method, request.params.nodeID);
            await checkNodeFragment(request, envelope.result, binding.principalID);
            let fragment: ResponseFragment;
            try {
              fragment = await fragments.createChunks(binding, request, envelope.result, snapshot.parts, snapshot.total, fragmentLimit, true);
              try {
                options.assertAuthority();
                const fresh = options.authentication.authenticate(token);
                if (fresh.principalID !== binding.principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before the response");
                authorizeNodeOperation(fresh, request.method, request.params.nodeID);
                await checkNodeFragment(request, envelope.result, binding.principalID);
              } catch (error) { fragments.release(fragment.id); throw error; }
            }
            catch (error) {
              if (error instanceof HarnessError) throw new HarnessError({ ...error.info, disposition: "uncertain" }, { cause: error });
              throw error;
            }
            sendFragment(outgoing, fragment, nodeBudgetHeaders); return;
          }
        }
        send(outgoing, 200, envelope, nodeSendLimit, false, request.method === "claim" ? nodeBudgetHeaders : undefined);
        return;
      }
      const request = validateRPCRequest(value);
      parsed = true;
      const requestedFragments = incoming.headers["harness-fragment-version"] === "2";
      const requestedLimit = incoming.headers["harness-fragment-limit"];
      if (requestedFragments && (typeof requestedLimit !== "string" || !/^[1-9][0-9]*$/.test(requestedLimit)
          || !Number.isSafeInteger(Number(requestedLimit)))) throw harnessError("INVALID_ARGUMENT", "Invalid fragment response budget");
      publicRequest = request;
      if (requestedFragments) publicFragmentLimit = Math.min(info.maxResponseBytes, Number(requestedLimit), 1_048_576);
      // Check after parsing so a fenced rejection still carries the original
      // request ID. The pre-body credential check never admits business work.
      try { options.assertAuthority(); }
      catch (error) {
        // A retried submit may already be durable. An unavailable authority
        // store cannot establish its command outcome at admission.
        if (request.method === "client.submit" && error instanceof HarnessError && error.info.code === "INTERNAL_ERROR") {
          throw new HarnessError({ ...error.info, disposition: "uncertain" }, { cause: error });
        }
        throw error;
      }
      // Body arrival is asynchronous. Re-read the configuration at dispatch, so a
      // credential revoked while the body was arriving does not authorize a call.
      const principal = options.authentication.authenticate(token);
      binding = { domainID: info.domainID, principalID: principal.principalID };
      if (request.binding && (request.binding.domainID !== binding.domainID || request.binding.principalID !== binding.principalID)) {
        throw harnessError("SESSION_MISMATCH", "Session binding does not match the current domain and authenticated principal");
      }
      authorizeOperation(principal, request.method);
      const responseLimit = request.method === "session.getInfo" && request.binding === null
        ? handshakeLimit(info, binding) : info.maxResponseBytes;
      if (!(requestedFragments && (request.method === "session.getInfo" || options.authorizeFragment && options.authorizeErrorFragment))) {
        assertEnvelopeCapacity(requestID!, binding, responseLimit);
      }
      let result: unknown;
      if (request.method === "session.getInfo") result = { ...info, principalID: principal.principalID, maxResponseBytes: responseLimit };
      else {
        possiblyCommitted = request.method === "client.submit" || request.method === "client.requestCancel" || request.method.startsWith("admin.");
        const revalidate = (): PrincipalGrant => {
          options.assertAuthority();
          const current = options.authentication.authenticate(token);
          if (current.principalID !== binding!.principalID) throw harnessError("SESSION_MISMATCH", "Session binding does not match the authenticated principal");
          authorizeOperation(current, request.method);
          return current;
        };
        try { result = await options.handle(request, principal, revalidate, { fragmentedResponses: requestedFragments && !!options.authorizeFragment }); }
        catch (error) {
          // Typed use-case errors carry their own commit certainty. An unexpected
          // failure after entering a mutation cannot establish rejection.
          if (error instanceof HarnessError) {
            try { validateErrorInfo(error.info); }
            catch { throw new Error("The operation produced an invalid infrastructure error"); }
            possiblyCommitted = false;
          }
          throw error;
        }
      }
      // A read-only handler may be asynchronous. Do not return its data after
      // takeover; mutations must retain their real receipt/commit certainty.
      if (!possiblyCommitted) options.assertAuthority();
      let envelope;
      try {
        envelope = validateRPCResponse({ protocolVersion: "harness/1", requestID, binding, ok: true, result }, request.method);
        if (!envelope.ok) throw new Error("Invalid success response");
      } catch {
        throw harnessError("INTERNAL_ERROR", "The operation produced an invalid public response");
      }
      if (requestedFragments && (request.method === "session.getInfo" || options.authorizeFragment)) {
        const chunkLimit = Math.min(responseLimit, Number(requestedLimit), 1_048_576);
        const snapshot = await measureResponseChunks(envelope);
        if (snapshot.total <= chunkLimit) {
          options.assertAuthority();
          const afterMeasure = options.authentication.authenticate(token);
          if (afterMeasure.principalID !== binding.principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before the response");
          authorizeOperation(afterMeasure, request.method);
          if (request.method !== "session.getInfo") await checkPublicFragment(request, envelope.result, binding.principalID, "result");
        }
        if (snapshot.total > chunkLimit) {
          options.assertAuthority();
          const current = options.authentication.authenticate(token);
          if (current.principalID !== binding.principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before the response");
          authorizeOperation(current, request.method);
          if (request.method !== "session.getInfo") await checkPublicFragment(request, envelope.result, binding.principalID, "result");
          const revalidate = () => publicPrincipal(request, binding!.principalID);
          const authorization = request.method !== "session.getInfo" && options.prepareFragmentAuthorization && options.authorizeFragmentDescriptor
            ? await options.prepareFragmentAuthorization(request, envelope.result, revalidate(), revalidate) : undefined;
          if (request.method !== "session.getInfo" && authorization !== undefined)
            await checkPublicFragment(request, authorization, binding.principalID, "descriptor");
          let fragment: ResponseFragment;
          try {
            fragment = await fragments.createChunks(binding, request, envelope.result, snapshot.parts, snapshot.total, chunkLimit, false, authorization);
            try {
              options.assertAuthority();
              const fresh = options.authentication.authenticate(token);
              if (fresh.principalID !== binding.principalID) throw harnessError("SESSION_MISMATCH", "Session binding changed before the response");
              authorizeOperation(fresh, request.method);
              if (request.method !== "session.getInfo") {
                if (authorization !== undefined && options.authorizeFragmentDescriptor) await checkPublicFragment(request, authorization, binding.principalID, "descriptor");
                else await checkPublicFragment(request, envelope.result, binding.principalID, "result");
              }
            } catch (error) { fragments.release(fragment.id); throw error; }
          }
          catch (error) {
            if (possiblyCommitted && error instanceof HarnessError) throw new HarnessError({ ...error.info, disposition: "uncertain" }, { cause: error });
            throw error;
          }
          sendFragment(outgoing, fragment, request.method === "session.getInfo" ? { [NODE_PROTOCOL_HEADER]: NODE_PROTOCOL_CAPABILITY,
            ...(options.advertiseBoundReportTransfer ? { "harness-report-transfer-version": "4" } : {}) } : undefined);
          return;
        }
      }
      send(outgoing, 200, envelope, responseLimit, request.method === "session.getInfo",
        request.method === "session.getInfo" ? { [NODE_PROTOCOL_HEADER]: NODE_PROTOCOL_CAPABILITY,
          ...(options.advertiseBoundReportTransfer ? { "harness-report-transfer-version": "4" } : {}) } : undefined);
    } catch (error) {
      let failure = toHarnessError(error, possiblyCommitted);
      if (possiblyCommitted && failure.info.disposition === "rejected") failure = new HarnessError({ ...failure.info, disposition: "uncertain" }, { cause: error });
      if (boundFailure && options.boundReportTransfer) {
        try {
          await authorizeBoundResponse(boundFailure.owner, boundFailure.transferID, token);
          const fragment = await boundErrors.create(boundFailure.owner, boundFailure.transferID, failure.info, Math.min(info.maxResponseBytes, 65_536));
          try { await authorizeBoundResponse(boundFailure.owner, boundFailure.transferID, token); }
          catch (error) { boundErrors.release(fragment.id); throw error; }
          const currentEntry = boundErrors.get(fragment.id);
          if (!currentEntry) throw harnessError("FORBIDDEN", "Error fragment is unavailable");
          const verifiedFragment = boundErrors.read(fragment.id, currentEntry, 0);
          if (verifiedFragment.total === verifiedFragment.bytes.length) {
            boundErrors.release(fragment.id);
            outgoing.writeHead(400, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": verifiedFragment.bytes.length });
            outgoing.end(verifiedFragment.bytes); return;
          }
          sendBoundErrorFragment(outgoing, verifiedFragment); return;
        } catch (fragmentError) {
          // The original scope may have been revoked or staging exhausted.
          // Return no protected details; a possible commit remains uncertain.
          const hash = createHash("sha256");
          for (const part of canonicalJsonChunks([boundFailure.owner.domainID, boundFailure.owner.principalID, boundFailure.owner.nodeID])) hash.update(part);
          let capacity = fragmentError instanceof HarnessError && fragmentError.info.code === "LIMIT_EXCEEDED";
          // A control upload may hit quota before a durable source binding
          // exists. Its generic backpressure error contains no source data;
          // still reauthenticate the worker before returning it.
          if (boundFailure.control && failure.info.code === "LIMIT_EXCEEDED") {
            try {
              options.assertAuthority();
              const current = options.authentication.authenticate(token);
              if (current.principalID !== boundFailure.owner.principalID || !current.roles.includes("worker")) throw new Error("worker changed");
              options.authorizeStagedWorker!(undefined, undefined, current);
              capacity = true;
            } catch { capacity = false; }
          }
          const safe = harnessError(capacity ? "LIMIT_EXCEEDED" : "FORBIDDEN",
            capacity ? "Report error snapshot capacity is unavailable" : "Report transfer is unavailable", null, possiblyCommitted ? "uncertain" : "rejected").info;
          const encoded = canonicalJson({ version: 4, transferID: boundFailure.transferID, workerDigest: hash.digest("hex"), ok: false, error: safe });
          if (Buffer.byteLength(encoded) <= info.maxResponseBytes) {
            outgoing.writeHead(403, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": Buffer.byteLength(encoded) });
            outgoing.end(encoded); return;
          }
        }
      }
      const status = failure.info.code === "UNAUTHENTICATED" ? 401
        : failure.info.code === "FORBIDDEN" ? 403
          : failure.info.code === "PROTOCOL_MISMATCH" ? 426
            : !parsed && failure.info.code === "LIMIT_EXCEEDED" ? 413 : parsed ? 200 : 400;
      const envelope = { protocolVersion: "harness/1", requestID, binding, ok: false, error: failure.info };
      if (status === 200 && publicRequest && publicFragmentLimit && binding && requestID && options.authorizeErrorFragment) {
        try {
          const body = canonicalJson(envelope);
          if (Buffer.byteLength(body, "utf8") > publicFragmentLimit) {
            options.assertAuthority();
            const current = options.authentication.authenticate(credential(incoming));
            if (current.principalID !== binding.principalID) throw new Error("Session changed");
            authorizeOperation(current, publicRequest.method);
            await checkPublicFragment(publicRequest, failure.info, binding.principalID, "error");
            sendFragment(outgoing, fragments.create(binding, publicRequest, failure.info, body, publicFragmentLimit, false, true));
            return;
          }
        } catch {
          // The original error may itself contain protected data. A revoked or
          // unverifiable fragment source must not fall through to that envelope.
          const safe = harnessError("FORBIDDEN", "Error response is unavailable", null, possiblyCommitted ? "uncertain" : "rejected").info;
          sendError(outgoing, 403, { protocolVersion: "harness/1", requestID, binding, ok: false, error: safe }, info.maxResponseBytes, true);
          if (!incoming.complete) incoming.resume();
          return;
        }
      }
      sendError(outgoing, status, envelope, info.maxResponseBytes, publicFragmentLimit !== undefined);
      if (!incoming.complete) incoming.resume();
    } finally { releaseFlight?.(); releaseReportBody?.(); }
  }

  async function authorizeBoundResponse(owner: { domainID: string; principalID: string; nodeID: string }, transferID: string, token: string): Promise<void> {
    const assertWorker = (nodeID?: string, incarnation?: string) => {
      options.assertAuthority();
      const current = options.authentication.authenticate(token);
      if (current.principalID !== owner.principalID || !current.roles.includes("worker")) throw harnessError("FORBIDDEN", "Report transfer is unavailable");
      if (nodeID !== undefined) authorizeNodeOperation(current, "reportAction", nodeID);
      options.authorizeStagedWorker!(nodeID, incarnation, current);
    };
    assertWorker();
    await options.boundReportTransfer!.authorizeResponse(owner, transferID, assertWorker);
    assertWorker();
  }

  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(port, options.host ?? "127.0.0.1", () => { server.off("error", reject); resolve(); });
    });
  } catch (error) {
    server.closeAllConnections();
    server.close(() => { /* No published listener on a failed start. */ });
    await nodeFragments.close();
    throw error;
  }
  const address = server.address();
  if (!address || typeof address === "string") throw harnessError("INTERNAL_ERROR", "RPC server did not acquire a TCP endpoint");
  const host = address.address.includes(":") ? `[${address.address}]` : address.address;
  let closing: Promise<void> | undefined;
  return {
    endpoint: `http://${host}:${address.port}`,
    close() {
      fragments.close();
      boundErrors.close();
      closing ??= new Promise<void>((resolve, reject) => {
        server.close(error => error ? reject(error) : resolve());
        server.closeAllConnections();
      }).then(() => nodeFragments.close());
      return closing;
    },
  };
}

function sendFragment(response: ServerResponse, fragment: ResponseFragment, extraHeaders?: Record<string, string>): void {
  if (response.destroyed) return;
  response.writeHead(200, { "content-type": "application/octet-stream", "cache-control": "no-store",
    "content-length": fragment.bytes.length, "harness-fragment-version": "2", "harness-fragment-json": "canonical-1", "harness-fragment-id": fragment.id,
    "harness-fragment-offset": String(fragment.offset), "harness-fragment-total": String(fragment.total), "harness-fragment-sha256": fragment.digest, ...extraHeaders });
  response.end(fragment.bytes);
}

function sendBoundErrorFragment(response: ServerResponse, fragment: BoundErrorFragment): void {
  if (response.destroyed) return;
  response.writeHead(200, { "content-type": "application/octet-stream", "cache-control": "no-store", "content-length": fragment.bytes.length,
    "harness-v4-error-version": "1", "harness-v4-error-id": fragment.id, "harness-v4-error-offset": String(fragment.offset),
    "harness-v4-error-total": String(fragment.total), "harness-v4-error-sha256": fragment.digest });
  response.end(fragment.bytes);
}

/** An authenticated principal whose binding cannot fit in any request can
 * still obtain its initial DomainInfo. Do not raise the budget for a usable
 * bound session: execution and query services enforce the domain budget. */
function handshakeLimit(info: Omit<DomainInfo, "principalID">, binding: SessionBinding): number {
  if (Buffer.byteLength(canonicalJson(binding), "utf8") <= info.maxPayloadBytes) return info.maxResponseBytes;
  // The request ID is part of a body already limited by maxPayloadBytes. Six
  // times that byte count bounds JSON string escaping; the max safe integer
  // reserves the largest possible decimal spelling of maxResponseBytes.
  const envelope = { protocolVersion: "harness/1", requestID: "", binding, ok: true,
    result: { ...info, principalID: binding.principalID, maxResponseBytes: Number.MAX_SAFE_INTEGER } };
  const fixedBytes = Buffer.byteLength(canonicalJson(envelope), "utf8");
  const idBudget = 6 * info.maxPayloadBytes;
  const limit = fixedBytes + idBudget;
  if (!Number.isSafeInteger(idBudget) || !Number.isSafeInteger(limit)) throw harnessError("INVALID_ARGUMENT", "Handshake response budget exceeds supported range");
  return Math.max(info.maxResponseBytes, limit);
}

/** An operation cannot be safely dispatched when even its empty success or
 * correlated error cannot fit. This check makes no claim about result size. */
function assertEnvelopeCapacity(requestID: string, binding: SessionBinding, limit: number): void {
  const failure = harnessError("LIMIT_EXCEEDED", "RPC response envelope exceeds the published response limit").info;
  const common = { protocolVersion: "harness/1", requestID, binding };
  if (Buffer.byteLength(canonicalJson({ ...common, ok: true, result: null })) > limit
      || Buffer.byteLength(canonicalJson({ ...common, ok: false, error: failure })) > limit) {
    throw harnessError("LIMIT_EXCEEDED", "RPC response envelope exceeds the published response limit");
  }
}

function copyDomainInfo(info: Omit<DomainInfo, "principalID">): Omit<DomainInfo, "principalID"> {
  const result = validateRPCResponse({ protocolVersion: "harness/1", requestID: "configuration", binding: { domainID: info.domainID, principalID: "configuration" }, ok: true,
    result: { ...info, principalID: "configuration" } }, "session.getInfo");
  if (!result.ok) throw harnessError("INVALID_ARGUMENT", "Invalid domain configuration");
  if (Buffer.byteLength(canonicalJson(result)) > info.maxResponseBytes) throw harnessError("INVALID_ARGUMENT", "Response limit must accommodate the domain handshake");
  const { principalID: _, ...copied } = result.result;
  return copied;
}

function credential(request: IncomingMessage): string {
  const authorization = request.headers.authorization;
  const count = request.rawHeaders.filter((_, index) => index % 2 === 0 && request.rawHeaders[index]!.toLowerCase() === "authorization").length;
  if (count !== 1 || typeof authorization !== "string" || !/^Bearer [^\s]+$/i.test(authorization)) {
    throw harnessError("UNAUTHENTICATED", "A currently valid Harness credential is required");
  }
  return authorization.slice(7);
}

function readBody(request: IncomingMessage, limit: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    const finish = (error?: Error): void => {
      request.off("data", data); request.off("end", end); request.off("error", failed); request.off("aborted", aborted);
      if (error) reject(error); else resolve(Buffer.concat(chunks, size));
    };
    const data = (chunk: Buffer): void => {
      size += chunk.length;
      if (size > limit) finish(harnessError("LIMIT_EXCEEDED", "RPC request exceeds the published payload limit"));
      else chunks.push(chunk);
    };
    const end = (): void => finish();
    const failed = (): void => finish(harnessError("INVALID_ARGUMENT", "RPC request body could not be read"));
    const aborted = (): void => failed();
    request.on("data", data); request.once("end", end); request.once("error", failed); request.once("aborted", aborted);
  });
}

async function measureResponseChunks(value: unknown): Promise<{ parts: Iterable<Uint8Array>; total: number; yielded: boolean }> {
  const parts = canonicalJsonChunks(value); let total = 0, count = 0, yielded = false;
  for (const part of parts) {
    total += part.byteLength;
    if (!Number.isSafeInteger(total)) throw harnessError("LIMIT_EXCEEDED", "Response exceeds the supported size");
    if (++count % 8 === 0) { yielded = true; await new Promise<void>(resolve => setImmediate(resolve)); }
  }
  return { parts, total, yielded };
}

function send(response: ServerResponse, status: number, envelope: unknown, limit: number, advertiseLimit = false, extraHeaders?: Record<string, string>): void {
  const body = canonicalJson(envelope);
  if (Buffer.byteLength(body) > limit) throw harnessError("LIMIT_EXCEEDED", "RPC response exceeds the published response limit");
  if (response.destroyed) return;
  response.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "content-length": Buffer.byteLength(body),
    ...(advertiseLimit ? { "harness-max-response-bytes": String(limit) } : {}), ...extraHeaders });
  response.end(body);
}

function sendError(response: ServerResponse, status: number, envelope: { protocolVersion: string; requestID: string | null; binding: SessionBinding | null; ok: boolean; error: ErrorInfo }, limit: number, allowBindingElision = false): void {
  try { send(response, status, envelope, limit); }
  catch {
    if (allowBindingElision && envelope.binding) {
      try { send(response, status, { ...envelope, binding: null }, limit); return; }
      catch { /* The correlated request ID or error can itself exceed the budget. */ }
    }
    // Extremely long request IDs/bindings must not turn the error path into an
    // oversized response or an unhandled rejection.
    try { send(response, status, { protocolVersion: "harness/1", requestID: null, binding: null, ok: false,
      error: harnessError("LIMIT_EXCEEDED", "RPC error response exceeds the published response limit", null, envelope.error.disposition).info }, limit); }
    catch { response.destroy(); }
  }
}
