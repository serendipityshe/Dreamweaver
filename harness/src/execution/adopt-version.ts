import { createHash } from "node:crypto";
import { assertJsonValueSize, canonicalJson, canonicalJsonChunks, canonicalJsonDigest, copyJson, harnessError, HarnessError, jsonEqual, parseJson,
  type AdoptVersionRequest, type ErrorInfo, type Json, type VersionPointer } from "../contract/index.js";
import { authorizeDefinition, authorizeOperation, type PrincipalGrant } from "../message/index.js";
import { type AbsenceGuard, type SQLWrite, type SQLiteStore } from "../persistence/index.js";
import { getVersionPointer } from "../evolution/index.js";
import { sameChildValue } from "../persistence/index.js";
import { stageCommandEvidence, encodeCommandEvidence, readCommandEvidenceRequest, readCommandEvidenceProjectionSync, commandEvidenceProjection, commandPublicationWrites, abandonCommandValue, type CommandEvidenceSource } from "../persistence/index.js";

const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });

/** Version directory, routing and default pointer are deliberately independent. */
export function createVersionService(store: SQLiteStore, clock: { now(): number }, limits: { maxValueBytes: number; maxResponseBytes: number; valueQuota?: number }) {
    function apply(request: AdoptVersionRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant, requestID: string, fragmentedResponses: boolean, source: CommandEvidenceSource | null = null): VersionPointer {
      const storedRequest = encodeCommandEvidence(request, source);
      for (;;) {
        store.assertAuthority();
        const grant = revalidate();
        authorizeOperation(grant, "admin.adoptVersion");
        authorizeDefinition(grant, request.kind, request.ref.id, request.ref.version);
        const prior = store.query("SELECT * FROM commands WHERE principal_id=? AND request_key=?", [principal.principalID, request.requestKey])[0];
        if (prior) {
          if (prior.operation === "adoptVersion") {
            const original = readCommandEvidenceRequest<AdoptVersionRequest>(store, prior.request_json);
            authorizeDefinition(grant, original.kind, original.ref.id, original.ref.version);
            if (!sameChildValue(original, request)) throw harnessError("REQUEST_CONFLICT", "Request key is bound to another command");
          } else throw harnessError("REQUEST_CONFLICT", "Request key is bound to another command");
          if (prior.status === "rejected" && prior.error_json !== null) throw new HarnessError(decode<ErrorInfo>(prior.error_json));
          if (prior.status !== "applied" || prior.result_json === null) throw harnessError("INTERNAL_ERROR", "Version receipt is unavailable", null, "uncertain");
          return readCommandEvidenceProjectionSync(store, prior.result_json) as unknown as VersionPointer;
        }
        assertJsonValueSize(request.evidence, limits.maxValueBytes);
        const pointer = getVersionPointer(store, { kind: request.kind, id: request.ref.id });
        const known = store.query("SELECT id FROM definition_catalog WHERE kind=? AND id=? AND version=?", [request.kind, request.ref.id, request.ref.version]).length > 0;
        const failure = pointer.revision !== request.expectedRevision || !Number.isSafeInteger(pointer.revision + 1)
          ? harnessError("REVISION_CONFLICT", "Version revision changed")
          : !known ? harnessError("VERSION_UNAVAILABLE", "Version is not registered", { missing: "definition", id: request.ref.id, version: request.ref.version }) : null;
        const at = clock.now();
        if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock");
        const result: VersionPointer = { kind: request.kind, id: request.ref.id, ref: request.ref,
          revision: pointer.revision + 1, evidence: request.evidence, updatedAt: at };
        if (!fragmentedResponses) {
          // Check the actual response and conservative future query envelopes
          // before any business write. History may need a next cursor.
          const binding = { domainID: store.domainID, principalID: principal.principalID };
          const queryID = Buffer.byteLength(canonicalJson(requestID)) > 38 ? requestID : "00000000-0000-0000-0000-000000000000";
          const envelope = (id: string, value: unknown) => ({ protocolVersion: "harness/1", requestID: id, binding, ok: true, result: value });
          const max = Number.MAX_SAFE_INTEGER;
          const cursorBody = Buffer.from(canonicalJson({ high: max, position: max })).toString("base64url");
          const cursor = `${cursorBody}.${"x".repeat(43)}`;
          const command = { requestKey: request.requestKey, operation: "adoptVersion", status: failure ? "rejected" : "applied", result: failure ? null : result, error: failure?.info ?? null, createdAt: max, updatedAt: max };
          const event = { id: "00000000-0000-0000-0000-000000000000", sequence: max, at: max, runID: null, actionID: null, attemptID: null, type: "version.adopted", data: { pointer: result, requestKey: request.requestKey, principalID: principal.principalID } };
          const replies = [envelope(queryID, command), ...(failure ? [{ protocolVersion: "harness/1", requestID, binding, ok: false, error: failure.info }]
            : [envelope(requestID, result), envelope(queryID, result), envelope(queryID, { items: [event], nextCursor: cursor })])];
          if (replies
            .some(value => Buffer.byteLength(canonicalJson(value)) > limits.maxResponseBytes)) {
            throw harnessError("LIMIT_EXCEEDED", "Version result cannot fit in the response limit");
          }
        }
        const fingerprint = canonicalJsonDigest(request);
        const storedEvidence = commandEvidenceProjection(source, request.evidence, [[]]);
        const storedResult = commandEvidenceProjection(source, result, [["evidence"]]);
        const writes: SQLWrite[] = [];
        const absent: AbsenceGuard[] = [];
        if (pointer.ref) {
          writes.push(failure
            ? write("UPDATE version_pointers SET revision=? WHERE kind=? AND id=? AND version=? AND revision=?", [pointer.revision, request.kind, request.ref.id, pointer.ref.version, pointer.revision])
            : write("UPDATE version_pointers SET version=?,revision=?,evidence_json=?,updated_at=? WHERE kind=? AND id=? AND version=? AND revision=?", [request.ref.version, result.revision, { json: storedEvidence }, at, request.kind, request.ref.id, pointer.ref.version, pointer.revision]));
        } else {
          absent.push({ table: "version_pointers", kind: request.kind, id: request.ref.id });
          if (!failure) writes.push(write("INSERT INTO version_pointers(kind,id,version,revision,evidence_json,updated_at) VALUES(?,?,?,?,?,?)", [request.kind, request.ref.id, request.ref.version, result.revision, { json: storedEvidence }, at]));
        }
        if (!known) absent.push({ table: "definition_catalog", kind: request.kind, id: request.ref.id, version: request.ref.version });
        writes.push(write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", [principal.principalID, request.requestKey, "adoptVersion", { json: storedRequest }, fingerprint, failure ? "rejected" : "applied", failure ? null : { json: storedResult }, failure ? { json: json(failure.info) } : null, at, at]), ...commandPublicationWrites(source, store.coordinatorEpoch));
        const subjectID = canonicalJson([request.kind, request.ref.id]);
        try {
          store.commit({ operationID: `adopt:${canonicalJson([principal.principalID, request.requestKey])}`, kind: failure ? "version.adopt_rejected" : "version.adopt", subjectID,
            request: storedRequest, writes, absent, result: failure ? null : storedResult, evidence: failure ? json(failure.info) : json({ principalID: principal.principalID }), ...(source ? { valuePublications: [source.value] } : {}),
            events: failure ? [] : [{ type: "version.adopted", subjectID, data: commandEvidenceProjection(source, { pointer: result, requestKey: request.requestKey, principalID: principal.principalID }, [["pointer", "evidence"]]) }] });
        } catch (error) {
          if (error instanceof HarnessError && error.info.disposition !== "uncertain" && ["REVISION_CONFLICT", "REQUEST_CONFLICT"].includes(error.info.code)) continue;
          throw error;
        }
        if (failure) throw failure;
        return result;
      }
    }
  return { adoptVersion(request: AdoptVersionRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant = () => principal, requestID = "00000000-0000-0000-0000-000000000000", fragmentedResponses = false): VersionPointer | Promise<VersionPointer> {
    let bytes = 0;
    for (const chunk of canonicalJsonChunks(request.evidence)) { bytes += chunk.length; if (bytes > 16_384) break; }
    if (bytes <= 16_384 || store.query("SELECT request_key FROM commands WHERE principal_id=? AND request_key=?", [principal.principalID, request.requestKey])[0]) return apply(request, principal, revalidate, requestID, fragmentedResponses);
    request = copyJson(request) as unknown as AdoptVersionRequest;
    return (async () => {
      const check = () => { const fresh = revalidate(); if (fresh.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Adoption principal changed"); authorizeOperation(fresh, "admin.adoptVersion"); authorizeDefinition(fresh, request.kind, request.ref.id, request.ref.version); };
      check(); assertJsonValueSize(request.evidence, limits.maxValueBytes);
      const source = await stageCommandEvidence(store, principal.principalID, "adoptVersion", request, canonicalJson([request.kind, request.ref.id]), `adopt:${canonicalJson([principal.principalID, request.requestKey])}`, limits.valueQuota ?? Number.MAX_SAFE_INTEGER, check);
      try { check(); return apply(request, principal, revalidate, requestID, fragmentedResponses, source); } finally { abandonCommandValue(store, source); }
    })();
  } };
}
