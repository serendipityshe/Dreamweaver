import { harnessError, parseJson, validateRPCResponse, type VersionPointer, type VersionQuery } from "../contract/index.js";
import type { SQLiteStore } from "../persistence/index.js";
import { readCommandEvidenceProjectionSync, readCommandProjection, commandEvidenceMetadata, type CommandEvidenceSource } from "../persistence/index.js";
import { canonicalJson, type AdoptVersionRequest, type Json } from "../contract/index.js";

/** Read the durable default only; default adoption belongs to T18. */
export function getVersionPointer(store: SQLiteStore, query: VersionQuery): VersionPointer {
  const row = store.query("SELECT kind,id,version,revision,evidence_json,updated_at FROM version_pointers WHERE kind=? AND id=?", [query.kind, query.id])[0];
  if (!row) return { ...query, ref: null, revision: 0, evidence: null, updatedAt: null };
  return checkedPointer(store, query, row, readCommandEvidenceProjectionSync(store, row.evidence_json));
}

export async function getVersionPointerAsync(store: SQLiteStore, query: VersionQuery): Promise<VersionPointer> {
  const row = store.query("SELECT kind,id,version,revision,evidence_json,updated_at FROM version_pointers WHERE kind=? AND id=?", [query.kind, query.id])[0];
  if (!row) return { ...query, ref: null, revision: 0, evidence: null, updatedAt: null };
  const evidence = await readCommandProjection(store, row.evidence_json);
  store.assertAuthority();
  if (!store.query("SELECT id FROM version_pointers WHERE kind=? AND id=? AND revision=? AND evidence_json=?", [query.kind, query.id, row.revision as number, row.evidence_json as string])[0]) throw harnessError("STATE_CONFLICT", "Version changed during query");
  return checkedPointer(store, query, row, evidence);
}

function checkedPointer(store: SQLiteStore, query: VersionQuery, row: ReturnType<SQLiteStore["query"]>[number], evidence: Json): VersionPointer {
  try {
    const stored = parseJson(row.evidence_json as string) as Record<string, Json>;
    if (stored && typeof stored === "object" && !Array.isArray(stored) && stored.encoding === "command-evidence-projection-v1") {
      const source = stored.source as unknown as CommandEvidenceSource;
      if (source?.operation !== "adoptVersion" || source.subjectID !== canonicalJson([row.kind, row.id])) throw new Error("Wrong pointer evidence target");
      const command = store.query("SELECT request_json,status FROM commands WHERE principal_id=? AND request_key=?", [source.principalID, source.requestKey])[0];
      if (!command || command.status !== "applied") throw new Error("Pointer evidence command is unavailable");
      const request = commandEvidenceMetadata(command.request_json) as unknown as AdoptVersionRequest;
      if (request.kind !== row.kind || request.ref.id !== row.id || request.ref.version !== row.version || request.expectedRevision + 1 !== row.revision) throw new Error("Pointer evidence version changed");
    }
    const pointer = {
      kind: row.kind, id: row.id, ref: { id: row.id, version: row.version },
      revision: row.revision, evidence, updatedAt: row.updated_at,
    };
    const checked = validateRPCResponse({ protocolVersion: "harness/1", requestID: "persistent-version",
      binding: { domainID: store.domainID, principalID: "coordinator" }, ok: true, result: pointer }, "client.getVersion");
    if (!checked.ok || checked.result.kind !== query.kind || checked.result.id !== query.id
        || checked.result.revision < 1) throw new Error("Invalid pointer");
    return checked.result;
  } catch {
    throw harnessError("INTERNAL_ERROR", "Stored version pointer is invalid");
  }
}
