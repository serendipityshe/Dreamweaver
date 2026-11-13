import { harnessError, type DefinitionDescriptor } from "../contract/index.js";
import type { SQLiteStore } from "../persistence/index.js";

/** Persistent knowledge and presently routable locations are deliberately separate. */
export function getDefinitionAvailability(store: SQLiteStore, definition: DefinitionDescriptor, now: number): {
  known: boolean; nodes: { nodeID: string; incarnation: string; expiresAt: number }[];
} {
  if (!Number.isSafeInteger(now) || now < 0) throw harnessError("INVALID_ARGUMENT", "Invalid directory timestamp");
  const known = store.query("SELECT kind FROM definition_catalog WHERE kind=? AND id=? AND version=?", [definition.kind, definition.ref.id, definition.ref.version]).length !== 0;
  const nodes = store.query(`SELECT s.node_id,s.incarnation,s.expires_at FROM node_definitions d JOIN node_sessions s ON s.incarnation=d.incarnation
    WHERE d.kind=? AND d.id=? AND d.version=? AND s.state='online' AND s.expires_at>? ORDER BY s.node_id LIMIT 256`, [definition.kind, definition.ref.id, definition.ref.version, now])
    .map(row => ({ nodeID: row.node_id as string, incarnation: row.incarnation as string, expiresAt: row.expires_at as number }));
  return { known, nodes };
}
