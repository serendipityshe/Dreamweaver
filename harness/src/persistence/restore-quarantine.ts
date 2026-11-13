import { lstatSync, realpathSync } from "node:fs";
import { resolve } from "node:path";
import { harnessError } from "../contract/index.js";

/** Host-owned restore provenance, outside the possibly rolled-back database.
 * Deliberately no release/auto-recovery operation: a snapshot is audit-only. */
export const restoreQuarantinePath = (path: string): string => `${resolve(path)}.restore-quarantine.json`;

export function assertNotRestoredSnapshot(path: string): void {
  const paths = new Set([resolve(path)]);
  try { paths.add(realpathSync(path)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  for (const candidate of paths) {
    try { lstatSync(restoreQuarantinePath(candidate)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; throw error; }
    throw harnessError("STATE_CONFLICT", "Restored snapshot is quarantined for offline audit; coordinator startup is prohibited");
  }
}
