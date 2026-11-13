/** Host-side runtime entry. Caller applications use the root SDK entry. */
import { startCoordinator as startInternalCoordinator, type CoordinatorOptions, type CoordinatorInspection } from "./bootstrap.js";

export { StaticAuthentication } from "./message/auth.js";
export type { PrincipalGrant, StaticCredential } from "./message/auth.js";
export type { CoordinatorOptions, CoordinatorInspection } from "./bootstrap.js";

export interface Coordinator {
  readonly endpoint: string;
  inspectRecovery(): CoordinatorInspection;
  /** Stops coordinator services and closes SQLite; repeated calls are safe.
   * Close worker and caller sessions before closing their coordinator. */
  close(): Promise<void>;
}

/** Starts the authenticated HTTP/SQLite coordinator. Defaults to loopback and
 * an available port. Resolves only after recovery and activation succeed.
 * The database must be a file; authentication is explicitly supplied by the host. */
export function startCoordinator(options: CoordinatorOptions): Promise<Coordinator> {
  return startInternalCoordinator(options);
}
