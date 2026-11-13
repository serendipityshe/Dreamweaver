import { encodeActionSnapshot } from "../persistence/index.js";
import { readActionCommandReceipt } from "../persistence/index.js";
﻿import { createHash } from "node:crypto";
import { canonicalJson, copyJson, harnessError, HarnessError, jsonEqual, parseJson,
  type ActionView, type ErrorInfo, type Json, type RecheckActionRequest } from "../contract/index.js";
import { authorizeOperation, type PrincipalGrant } from "../message/index.js";
import { cancellationGuard, readActionSnapshot, type ActionControl, type SQLWrite, type SQLiteStore } from "../persistence/index.js";

const json = (value: unknown): Json => copyJson(value);
const decode = <T>(value: unknown): T => parseJson(value as string) as T;
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });

/** Explicit, durable recheck admission; never authorizes execution. */
export function createRecheckService(store: SQLiteStore, options: { clock: { now(): number }; authorizeAction(id: string, principal: PrincipalGrant): ActionView;
  authorizeActionControl?(id: string, principal: PrincipalGrant): ActionControl }) {
  return {
    async recheckAction(request: RecheckActionRequest, principal: PrincipalGrant, revalidate: () => PrincipalGrant = () => principal): Promise<ActionView> {
      for (;;) {
        store.assertAuthority();
        const grant = revalidate();
        authorizeOperation(grant, "admin.recheckAction");
        const prior = store.query("SELECT * FROM commands WHERE principal_id=? AND request_key=?", [principal.principalID, request.requestKey])[0];
        if (prior) {
          const original = decode<RecheckActionRequest>(prior.request_json);
          const targetID = prior.operation === "recheckAction" ? original.actionID : request.actionID;
          const targetRow = store.query("SELECT * FROM actions WHERE id=?", [targetID])[0];
          if (targetRow?.snapshot_projection_id != null) {
            if (!options.authorizeActionControl) throw harnessError("STATE_CONFLICT", "Referenced Action authorization is unavailable");
            const before = options.authorizeActionControl(targetID, grant);
            await readActionSnapshot(store, targetRow);
            const fresh = revalidate(); authorizeOperation(fresh, "admin.recheckAction");
            if (fresh.principalID !== principal.principalID || options.authorizeActionControl(targetID, fresh).revision !== before.revision)
              throw harnessError("STATE_CONFLICT", "Action changed during recheck authorization");
          } else options.authorizeAction(targetID, grant);
          if (prior.operation !== "recheckAction" || !jsonEqual(original, request)) throw harnessError("REQUEST_CONFLICT", "Request key is bound to another command");
          if (prior.status === "rejected" && prior.error_json !== null) throw new HarnessError(decode<ErrorInfo>(prior.error_json));
          if (prior.status !== "applied" || prior.result_json === null) throw harnessError("INTERNAL_ERROR", "Recheck receipt is unavailable", null, "uncertain");
          return readActionCommandReceipt(store, prior.result_json, original.actionID);
        }
        const row = store.query("SELECT * FROM actions WHERE id=?", [request.actionID])[0];
        if (!row?.snapshot_json) throw harnessError("NOT_FOUND", "Action not found");
        if (row.snapshot_projection_id !== null) {
          if (!options.authorizeActionControl) throw harnessError("STATE_CONFLICT", "Referenced Action authorization is unavailable");
          const control = options.authorizeActionControl(request.actionID, grant);
          // Rechecks of a cancelled Action can still be meaningful; they need
          // the full reference-aware writer and remain gated until it exists.
          if (control.outcomeKind === null || control.outcomeKind === "cancelled") throw harnessError("STATE_CONFLICT", "Referenced Action recheck is not yet available");
          const verified = await readActionSnapshot(store, row);
          const fresh = revalidate(); authorizeOperation(fresh, "admin.recheckAction");
          if (fresh.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Session principal changed during recheck");
          const current = options.authorizeActionControl(request.actionID, fresh);
          if (current.revision !== control.revision || verified.revision !== control.revision || row.revision !== control.revision) continue;
          const work = store.query("SELECT * FROM work_items WHERE kind='action' AND subject_id=?", [control.id])[0];
          const at = options.clock.now();
          if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock");
          const lease = store.query("SELECT * FROM business_leases WHERE kind='action' AND subject_id=?", [control.id])[0];
          const active = lease && lease.released === 0 && (lease.expires_at as number) > at;
          const capability = control.kind === "capacity" ? store.query("SELECT supported FROM capacity_reconciliation_support WHERE id=? AND version=?", [control.target.id, control.target.version])[0] : undefined;
          const failure = control.revision !== request.expectedRevision ? harnessError("REVISION_CONFLICT", "Action revision changed")
            : control.kind === "agent" ? harnessError("INVALID_ARGUMENT", "Agent Action must be managed through its child Run")
            : control.recheckRequested || active || work?.stage === "recheck" || work?.stage === "running" || work?.stage === "report_pending" ? harnessError("STATE_CONFLICT", "Action already has a queued or active round")
            : harnessError("ACTION_TERMINAL", "Action has no unresolved effect");
          const writes: SQLWrite[] = [write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS ?", [control.id, control.revision, row.snapshot_json as string, row.snapshot_projection_id as string]),
            ...(work ? [write("UPDATE work_items SET stage=? WHERE id=? AND kind='action' AND subject_id=? AND stage=?", [work.stage as string, work.id as string, control.id, work.stage as string])] : []),
            write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
              [principal.principalID, request.requestKey, "recheckAction", { json: json(request) }, createHash("sha256").update(canonicalJson(request)).digest("hex"), "rejected", null, { json: json(failure.info) }, at, at])];
          writes.unshift(write("UPDATE actions SET epoch=epoch+0 WHERE id=? AND epoch=?", [control.id, row.epoch as number]));
          if (lease) writes.unshift(write("UPDATE business_leases SET expires_at=expires_at+0 WHERE kind='action' AND subject_id=? AND epoch=? AND released=? AND expires_at=?", [control.id, lease.epoch as number, lease.released as number, lease.expires_at as number]));
          if (capability) writes.unshift(write("UPDATE capacity_reconciliation_support SET revision=revision+0 WHERE id=? AND version=? AND supported=?", [control.target.id, control.target.version, capability.supported as number]));
          try {
            store.commit({ operationID: `recheck:${canonicalJson([principal.principalID, request.requestKey])}`, kind: "action.recheck_rejected", subjectID: control.id,
              request: json(request), writes, result: null, evidence: json(failure.info),
              ...(!capability && control.kind === "capacity" ? { absent: [{ table: "capacity_reconciliation_support" as const, id: control.target.id, version: control.target.version }] } : {}),
              ...(active && failure.info.code === "STATE_CONFLICT" ? { validBefore: lease!.expires_at as number } : {}) });
          } catch (error) {
            if (error instanceof HarnessError && error.info.disposition !== "uncertain" && ["REVISION_CONFLICT", "REQUEST_CONFLICT", "EXECUTION_FENCED"].includes(error.info.code)) continue;
            throw error;
          }
          throw failure;
        }
        const projected = options.authorizeAction(request.actionID, grant);
        const action = await readActionSnapshot(store, row);
        const refreshed = revalidate();
        authorizeOperation(refreshed, "admin.recheckAction");
        if (refreshed.principalID !== principal.principalID) throw harnessError("SESSION_MISMATCH", "Session principal changed during recheck");
        const currentProjection = options.authorizeAction(request.actionID, refreshed);
        if (currentProjection.revision !== action.revision) continue;
        if (action.revision !== row.revision || projected.revision !== action.revision) continue;
        const work = store.query("SELECT * FROM work_items WHERE kind='action' AND subject_id=?", [action.id])[0];
        const at = options.clock.now();
        if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator clock");
        const lease = store.query("SELECT * FROM business_leases WHERE kind='action' AND subject_id=?", [action.id])[0];
        const active = lease && lease.released === 0 && (lease.expires_at as number) > at;
        const capability = action.request.kind === "capacity" ? store.query("SELECT supported FROM capacity_reconciliation_support WHERE id=? AND version=?", [action.request.target.id, action.request.target.version])[0] : undefined;
        const support = capability ? capability.supported === 1 : undefined;
        const failure = action.revision !== request.expectedRevision ? harnessError("REVISION_CONFLICT", "Action revision changed")
          : action.request.kind === "agent" ? harnessError("INVALID_ARGUMENT", "Agent Action must be managed through its child Run")
          : action.recheckRequested || active || work?.stage === "recheck" || work?.stage === "running" || work?.stage === "report_pending" ? harnessError("STATE_CONFLICT", "Action already has a queued or active round")
          : action.outcome && (action.outcome.kind !== "cancelled" || !action.blocked || action.executionCount === 0) ? harnessError("ACTION_TERMINAL", "Action has no unresolved effect")
          : !action.outcome && !action.blocked ? harnessError("STATE_CONFLICT", "Action is not blocked")
          : !work || !["blocked", "done"].includes(work.stage as string) ? harnessError("STATE_CONFLICT", "Action work is not paused")
          : support === false ? harnessError("RECONCILIATION_REQUIRED", "Fixed Capacity does not support reconciliation")
          : support === undefined ? harnessError("VERSION_UNAVAILABLE", "Reconciliation support has not been registered") : null;
        const fingerprint = createHash("sha256").update(canonicalJson(request)).digest("hex");
        const next: ActionView = { ...action, revision: action.revision + 1, updatedAt: at, recheckRequested: true };
        const receipt: ActionView = currentProjection.cancelRequested && !action.outcome ? { ...next, cancelRequested: true, cancellation: currentProjection.cancellation } : next;
        const writes: SQLWrite[] = [write("UPDATE actions SET revision=revision+0 WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [action.id, action.revision, row.snapshot_json as string]),
          ...(work ? [write("UPDATE work_items SET stage=? WHERE id=? AND kind='action' AND subject_id=? AND stage=?", [work.stage as string, work.id as string, action.id, work.stage as string])] : []),
          ...(!action.outcome && !currentProjection.cancelRequested ? [cancellationGuard({ kind: "action", id: action.id })] : []),
          write("INSERT INTO commands(principal_id,request_key,operation,request_json,fingerprint,status,result_json,error_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", [principal.principalID, request.requestKey, "recheckAction", { json: json(request) }, fingerprint, failure ? "rejected" : "applied", failure ? null : { json: encodeActionSnapshot(receipt) }, failure ? { json: json(failure.info) } : null, at, at])];
        // Lease creation changes Action epoch; existing lease renewal/release
        // must also be checked in the same transaction as the receipt.
        writes.unshift(write("UPDATE actions SET epoch=epoch+0 WHERE id=? AND epoch=?", [action.id, row.epoch as number]));
        if (lease) writes.unshift(write("UPDATE business_leases SET expires_at=expires_at+0 WHERE kind='action' AND subject_id=? AND epoch=? AND released=? AND expires_at=?", [action.id, lease.epoch as number, lease.released as number, lease.expires_at as number]));
        if (capability) writes.unshift(write("UPDATE capacity_reconciliation_support SET revision=revision+0 WHERE id=? AND version=? AND supported=?", [action.request.target.id, action.request.target.version, capability.supported as number]));
        if (!failure && work) {
          writes.push(write("UPDATE actions SET revision=?,snapshot_json=? WHERE id=? AND revision=? AND snapshot_json=? AND snapshot_projection_id IS NULL", [next.revision, { json: encodeActionSnapshot(next) }, action.id, action.revision, row.snapshot_json as string]),
            write("UPDATE work_items SET stage=?,available_at=? WHERE id=? AND kind='action' AND subject_id=? AND stage=?", ["recheck", at, work.id as string, action.id, work.stage as string]));
          const old = store.query("SELECT * FROM action_recheck_requests WHERE action_id=?", [action.id])[0];
          writes.push(old ? write("UPDATE action_recheck_requests SET principal_id=?,request_key=? WHERE action_id=? AND principal_id=? AND request_key=?", [principal.principalID, request.requestKey, action.id, old.principal_id as string, old.request_key as string])
            : write("INSERT INTO action_recheck_requests(action_id,principal_id,request_key) VALUES(?,?,?)", [action.id, principal.principalID, request.requestKey]));
        }
        try {
          store.commit({ operationID: `recheck:${canonicalJson([principal.principalID, request.requestKey])}`, kind: failure ? "action.recheck_rejected" : "action.recheck_requested", subjectID: action.id,
            request: json(request), writes, result: failure ? null : encodeActionSnapshot(receipt), evidence: failure ? json(failure.info) : json({ principalID: principal.principalID, reason: request.reason }),
            ...(!capability && action.request.kind === "capacity" ? { absent: [{ table: "capacity_reconciliation_support" as const, id: action.request.target.id, version: action.request.target.version }] } : {}),
            events: failure ? [] : [{ type: "action.recheck_requested", subjectID: action.id, data: json({ principalID: principal.principalID, requestKey: request.requestKey, reason: request.reason }) }],
            ...(active && failure?.info.code === "STATE_CONFLICT" ? { validBefore: lease!.expires_at as number } : {}) });
        } catch (error) {
          if (error instanceof HarnessError && error.info.disposition !== "uncertain" && ["REVISION_CONFLICT", "REQUEST_CONFLICT", "EXECUTION_FENCED"].includes(error.info.code)) continue;
          throw error;
        }
        if (failure) throw failure;
        return receipt;
      }
    },
  };
}
