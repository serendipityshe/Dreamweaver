import {
  copyJson, harnessError, type DefinitionKind, type NodeMethod, type PublicMethod,
} from "../contract/index.js";

export interface PrincipalGrant {
  principalID: string;
  roles: ("client" | "admin" | "worker")[];
  operations: PublicMethod[];
  definitions: { kind: DefinitionKind | "policy"; id: string; versions?: string[] | "*" }[];
  nodeIDs?: string[] | "*";
  nodeOperations?: NodeMethod[];
  managedPrincipalIDs?: string[] | "*";
  domainVersion?: boolean;
}
export interface StaticCredential { token: string; grant: PrincipalGrant }

const methods = new Set<PublicMethod>([
  "session.getInfo", "client.submit", "client.getRun", "client.getRunByRequestKey",
  "client.getAction", "client.listActions", "client.requestCancel", "client.getCommand",
  "client.getVersion", "client.getHistory", "admin.resumeRun", "admin.recheckAction",
  "admin.resolveAction", "admin.adoptVersion",
]);
const nodeMethods = new Set<NodeMethod>(["register", "renew", "quiesce", "claim", "report", "unregister", "renewLease", "verifyLease", "agentExecution", "verifyValidation", "readValidationValue", "releaseLease", "dispatchAction", "actionContinuation", "checkpointAction", "readCheckpoint", "reportAction", "diagnoseAction"]);
const identifier = (value: unknown): value is string => typeof value === "string" && value.length > 0 && value.trim() === value;
const identifiers = (value: unknown): value is string[] => Array.isArray(value) && value.every(identifier);
const fields = (value: object, allowed: string[]): boolean => Object.keys(value).every(key => allowed.includes(key));

/** Host-owned credential mapping. Replacement takes effect on the next request. */
export class StaticAuthentication {
  #credentials = new Map<string, PrincipalGrant>();

  constructor(entries: StaticCredential[]) { this.replace(entries); }

  replace(entries: StaticCredential[]): void {
    const copied = copyJson(entries) as unknown as StaticCredential[];
    if (!Array.isArray(copied)) throw harnessError("INVALID_ARGUMENT", "Invalid static authentication configuration");
    const next = new Map<string, PrincipalGrant>();
    for (const entry of copied) {
      const grant = entry?.grant;
      if (!entry || !fields(entry, ["token", "grant"]) || !identifier(entry.token) || !/^[\x21-\x7e]+$/.test(entry.token)
          || next.has(entry.token) || !grant || !fields(grant, ["principalID", "roles", "operations", "definitions", "managedPrincipalIDs", "domainVersion", "nodeIDs", "nodeOperations"])
          || !identifier(grant.principalID) || !Array.isArray(grant.roles) || grant.roles.length === 0
          || !grant.roles.every(role => ["client", "admin", "worker"].includes(role))
          || !Array.isArray(grant.operations) || !grant.operations.every(method => methods.has(method))
          || !Array.isArray(grant.definitions) || !grant.definitions.every(definition => definition && fields(definition, ["kind", "id", "versions"])
            && ["agent", "capacity", "policy"].includes(definition.kind) && identifier(definition.id)
            && (definition.versions === undefined || definition.versions === "*" || identifiers(definition.versions)))
          || (grant.managedPrincipalIDs !== undefined && grant.managedPrincipalIDs !== "*" && !identifiers(grant.managedPrincipalIDs))
          || (grant.nodeIDs !== undefined && grant.nodeIDs !== "*" && !identifiers(grant.nodeIDs))
          || (grant.nodeOperations !== undefined && (!Array.isArray(grant.nodeOperations) || !grant.nodeOperations.every(method => nodeMethods.has(method))))
          || (grant.domainVersion !== undefined && typeof grant.domainVersion !== "boolean")) {
        throw harnessError("INVALID_ARGUMENT", "Invalid static authentication configuration");
      }
      next.set(entry.token, grant);
    }
    this.#credentials = next;
  }

  authenticate(token: string): PrincipalGrant {
    const grant = this.#credentials.get(token);
    if (!grant) throw harnessError("UNAUTHENTICATED", "A currently valid Harness credential is required");
    // An injected handler cannot mutate the configuration used by later requests.
    return copyJson(grant) as unknown as PrincipalGrant;
  }

  /** Worker credentials never confer the original business principal's rights. */
  authorizePrincipal(principalID: string, operation: PublicMethod, kind: DefinitionKind, ref: { id: string; version?: string }): void {
    for (const grant of this.#credentials.values()) {
      if (grant.principalID !== principalID) continue;
      try { authorizeOperation(grant, operation); authorizeDefinition(grant, kind, ref.id, ref.version); return; }
      catch { /* A separate active credential may carry the complete grant. */ }
    }
    throw harnessError("FORBIDDEN", "The original business principal is no longer authorized");
  }

  /** Recheck the current worker credential; one grant must cover the node and its assembled definitions. */
  authorizeWorkerPrincipal(principalID: string, nodeID: string, definitions: { kind: DefinitionKind | "policy"; ref: { id: string; version: string } }[]): void {
    for (const grant of this.#credentials.values()) {
      if (grant.principalID !== principalID) continue;
      try {
        authorizeNodeOperation(grant, "claim", nodeID);
        authorizeNodeOperation(grant, "report", nodeID);
        for (const definition of definitions) authorizeDefinition(grant, definition.kind, definition.ref.id, definition.ref.version);
        return;
      } catch { /* A separate active credential may carry the complete worker grant. */ }
    }
    throw harnessError("FORBIDDEN", "The worker is no longer authorized");
  }

  authorizeManagementPrincipal(principalID: string, ownerID: string, kind: DefinitionKind, ref: { id: string; version: string }, operation: "admin.resolveAction" | "admin.recheckAction" = "admin.resolveAction"): void {
    for (const grant of this.#credentials.values()) {
      if (grant.principalID !== principalID) continue;
      try {
        authorizeOperation(grant, operation);
        if (!grant.roles.includes("admin") || grant.managedPrincipalIDs !== "*" && !grant.managedPrincipalIDs?.includes(ownerID)) continue;
        authorizeDefinition(grant, kind, ref.id, ref.version);
        return;
      } catch { /* A different current credential may carry the complete grant. */ }
    }
    throw harnessError("FORBIDDEN", "The management principal is not authorized for this Action");
  }
}

export function authorizeOperation(grant: PrincipalGrant, method: PublicMethod): void {
  if (method === "session.getInfo") return;
  const roleAllowed = method === "client.submit" ? grant.roles.includes("client") : method.startsWith("admin.")
    ? grant.roles.includes("admin")
    : grant.roles.includes("client") || grant.roles.includes("admin");
  if (!roleAllowed || !grant.operations.includes(method) || method === "admin.adoptVersion" && grant.domainVersion !== true) {
    throw harnessError("FORBIDDEN", "The current role and operation grant do not allow this request");
  }
}

export function authorizeDefinition(grant: PrincipalGrant, kind: DefinitionKind | "policy", id: string, version?: string): void {
  if (!grant.definitions.some(definition => definition.kind === kind && (definition.id === id || definition.id === "*")
      && (version === undefined || definition.versions === undefined || definition.versions === "*" || definition.versions.includes(version)))) {
    throw harnessError("FORBIDDEN", "The target definition is not authorized");
  }
}

export function authorizeNodeOperation(grant: PrincipalGrant, method: NodeMethod, nodeID: string): void {
  const authorizedMethod = method === "quiesce" || method === "renewLease" || method === "verifyLease" || method === "agentExecution" || method === "verifyValidation" || method === "readValidationValue" || method === "releaseLease" || method === "actionContinuation" || method === "checkpointAction" || method === "readCheckpoint" ? "renew"
    : method === "dispatchAction" ? "claim" : method === "reportAction" || method === "diagnoseAction" ? "report" : method;
  if (!grant.roles.includes("worker") || !(grant.nodeOperations?.includes(method) || grant.nodeOperations?.includes(authorizedMethod))
      || grant.nodeIDs !== "*" && !grant.nodeIDs?.includes(nodeID)) {
    throw harnessError("FORBIDDEN", "The worker operation or node is not authorized");
  }
}
