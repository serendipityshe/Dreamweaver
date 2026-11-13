import { createHash, randomUUID } from "node:crypto";
import { canonicalJson, harnessError, parseJson, type Json } from "../contract/index.js";
import { cancellationGuard, readActionControl, type SQLiteStore, type SQLWrite, type CommitBatch } from "../persistence/index.js";

type Kind = "run" | "action";
export interface LeaseGrant {
  kind: Kind; subjectID: string; nodeID: string; workerPrincipalID: string; incarnation: string;
  epoch: number; expiresAt: number; attemptID: string | null; attemptNumber: number | null;
}
export interface ClaimRequest { kind: Kind; subjectID: string; nodeID: string; workerPrincipalID: string; incarnation: string; claimRequestID: string }
type Row = ReturnType<SQLiteStore["query"]>[number];
const write = (sql: string, params: SQLWrite["params"]): SQLWrite => ({ sql, params, expectChanges: 1 });
const id = (s: unknown): s is string => typeof s === "string" && s.length > 0 && s.trim() === s;
const op = (type: string, parts: string[]) => `lease.${type}:${createHash("sha256").update(canonicalJson(parts)).digest("hex")}`;
const value = (x: unknown): Json => JSON.parse(canonicalJson(x)) as Json;

/** Private execution authority. Short validation assignments and coordinator epochs are separate. */
export class BusinessLeaseService {
  private readonly clock: { now(): number };
  private readonly ttlMs: number;
  private readonly authorizeRecheck?: (actionID: string) => SQLWrite;
  constructor(private readonly store: SQLiteStore, options: { clock: { now(): number }; ttlMs: number; authorizeRecheck?: (actionID: string) => SQLWrite }) {
    if (!options || !options.clock || typeof options.clock.now !== "function" || !Number.isSafeInteger(options.ttlMs) || options.ttlMs <= 0)
      throw harnessError("INVALID_ARGUMENT", "Invalid business lease configuration");
    this.clock = options.clock; this.ttlMs = options.ttlMs; this.authorizeRecheck = options.authorizeRecheck;
  }

  claim(request: ClaimRequest): LeaseGrant | null {
    this.checkRequest(request);
    const operationID = op("claim", [request.incarnation, request.claimRequestID]);
    const prior = this.replay(operationID, "lease.claim", request.nodeID, request);
    this.session(request, this.now());
    if (prior.found) {
      const grant = prior.result as unknown as LeaseGrant | null;
      if (grant) this.assert(grant);
      return grant;
    }
    const prepared = this.prepareClaim(request);
    const receipt = this.store.commit({ operationID, kind: "lease.claim", subjectID: request.nodeID, request: value(request),
      writes: prepared.writes, ...(prepared.absent ? { absent: prepared.absent } : {}),
      evidence: value(prepared.grant), result: value(prepared.grant), validBefore: prepared.validBefore });
    const result = receipt.result as unknown as LeaseGrant | null;
    if (result) this.assert(result);
    return result;
  }

  /** Construct lease writes for inclusion in an enclosing claim transaction. The
   * caller must persist and replay its own complete result, then assert the grant. */
  prepareClaim(request: ClaimRequest, recheck = false): { grant: LeaseGrant | null; writes: SQLWrite[]; validBefore: number; absent?: CommitBatch["absent"] } {
    this.checkRequest(request);
    const now = this.now();
    const session = this.session(request, now);
    const table = request.kind === "run" ? "runs" : "actions";
    const subject = this.store.query(`SELECT epoch FROM ${table} WHERE id=?`, [request.subjectID])[0];
    const current = this.store.query("SELECT * FROM business_leases WHERE kind=? AND subject_id=?", [request.kind, request.subjectID])[0];
    const available = !!subject && (!current || current.released === 1 || (current.expires_at as number) <= now);
    const writes: SQLWrite[] = [this.sessionGuard(request, session, now)];
    let grant: LeaseGrant | null = null;
    if (available) {
      const epoch = (subject!.epoch as number) + 1;
      if (!Number.isSafeInteger(epoch)) throw harnessError("STATE_CONFLICT", "Business lease epoch exhausted");
      const expiresAt = Math.min(now + this.ttlMs, session.expires_at as number);
      if (!Number.isSafeInteger(expiresAt)) throw harnessError("INVALID_ARGUMENT", "Invalid business lease deadline");
      const attemptNumber = request.kind === "action" ? Number(this.store.query("SELECT COALESCE(MAX(number),0)+1 n FROM business_attempts WHERE action_id=?", [request.subjectID])[0]!.n) : null;
      grant = { kind: request.kind, subjectID: request.subjectID, nodeID: request.nodeID, workerPrincipalID: request.workerPrincipalID, incarnation: request.incarnation,
        epoch, expiresAt, attemptID: request.kind === "action" ? randomUUID() : null, attemptNumber };
      writes.push(write(`UPDATE ${table} SET epoch=? WHERE id=? AND epoch=?`, [epoch, request.subjectID, subject!.epoch as number]));
      if (current) writes.push(write(`UPDATE business_leases SET epoch=?,node_id=?,worker_principal_id=?,incarnation=?,expires_at=?,released=0,attempt_id=?,attempt_number=?
        WHERE kind=? AND subject_id=? AND epoch=? AND released=? AND expires_at=?`,
        [epoch, request.nodeID, request.workerPrincipalID, request.incarnation, expiresAt, grant.attemptID, attemptNumber, request.kind, request.subjectID, current.epoch as number, current.released as number, current.expires_at as number]));
      else writes.push(write("INSERT INTO business_leases(kind,subject_id,epoch,node_id,worker_principal_id,incarnation,expires_at,released,attempt_id,attempt_number) VALUES(?,?,?,?,?,?,?,0,?,?)", [request.kind, request.subjectID, epoch, request.nodeID, request.workerPrincipalID, request.incarnation, expiresAt, grant.attemptID, attemptNumber]));
      if (grant.attemptID) writes.push(write("INSERT INTO business_attempts(id,action_id,number,epoch,node_id,worker_principal_id,incarnation,started_at) VALUES(?,?,?,?,?,?,?,?)", [grant.attemptID, request.subjectID, attemptNumber, epoch, request.nodeID, request.workerPrincipalID, request.incarnation, now]));
      writes.push(...this.guard(grant, recheck));
    }
    // An empty receipt must describe an unavailable subject at the write lock,
    // not merely a stale pre-transaction read.
    if (subject && !available && current) writes.push(write("UPDATE business_leases SET expires_at=expires_at+0 WHERE kind=? AND subject_id=? AND epoch=? AND released=0 AND expires_at=?", [request.kind, request.subjectID, current.epoch as number, current.expires_at as number]));
    return { grant, writes, ...(!subject ? { absent: [{ table, id: request.subjectID } as const] } : {}),
      validBefore: grant?.expiresAt ?? Math.min(session.expires_at as number, current ? current.expires_at as number : Number.MAX_SAFE_INTEGER) };
  }

  renew(request: LeaseGrant & { renewRequestID: string }): LeaseGrant {
    if (!id(request.renewRequestID)) throw harnessError("INVALID_ARGUMENT", "Invalid renewal identity");
    const { renewRequestID, ...grant } = request;
    this.checkGrant(grant);
    const operationID = op("renew", [grant.incarnation, renewRequestID]);
    const prior = this.replay(operationID, "lease.renew", grant.nodeID, request);
    const now = this.now(), session = this.session(grant, now);
    if (prior.found) {
      const renewed = prior.result as unknown as LeaseGrant;
      this.assert(renewed);
      return renewed;
    }
    const expiresAt = Math.min(now + this.ttlMs, session.expires_at as number);
    if (!Number.isSafeInteger(expiresAt)) throw harnessError("INVALID_ARGUMENT", "Invalid business lease deadline");
    const renewed = { ...grant, expiresAt };
    const receipt = this.store.commit({ operationID, kind: "lease.renew", subjectID: grant.nodeID, request: value(request),
      writes: [this.sessionGuard(grant, session, now), ...this.guard(grant), write("UPDATE business_leases SET expires_at=? WHERE kind=? AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at=? AND released=0", [expiresAt, grant.kind, grant.subjectID, grant.nodeID, grant.workerPrincipalID, grant.incarnation, grant.epoch, grant.expiresAt])],
      evidence: value(renewed), result: value(renewed), validBefore: grant.expiresAt });
    const result = receipt.result as unknown as LeaseGrant;
    this.assert(result);
    return result;
  }

  release(request: LeaseGrant & { releaseRequestID: string }): void {
    if (!id(request.releaseRequestID)) throw harnessError("INVALID_ARGUMENT", "Invalid release identity");
    const { releaseRequestID, ...grant } = request;
    this.checkGrant(grant);
    const operationID = op("release", [grant.incarnation, releaseRequestID]);
    const prior = this.replay(operationID, "lease.release", grant.nodeID, request);
    const now = this.now(), session = this.session(grant, now);
    if (prior.found) {
      const row = this.store.query("SELECT node_id,worker_principal_id,incarnation,epoch,expires_at,released,attempt_id,attempt_number FROM business_leases WHERE kind=? AND subject_id=?", [grant.kind, grant.subjectID])[0];
      if (!row || row.node_id !== grant.nodeID || row.worker_principal_id !== grant.workerPrincipalID || row.incarnation !== grant.incarnation || row.epoch !== grant.epoch || (row.expires_at as number) < grant.expiresAt || row.released !== 1 || row.attempt_id !== grant.attemptID || row.attempt_number !== grant.attemptNumber || grant.expiresAt <= now) throw harnessError("EXECUTION_FENCED", "Released business lease is no longer current");
      return;
    }
    const table = grant.kind === "run" ? "runs" : "actions";
    this.store.commit({ operationID, kind: "lease.release", subjectID: grant.nodeID, request: value(request),
      writes: [this.sessionGuard(grant, session, now), write(`UPDATE ${table} SET epoch=epoch+0 WHERE id=? AND epoch=?`, [grant.subjectID, grant.epoch]), write("UPDATE business_leases SET released=1 WHERE kind=? AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at>=? AND expires_at>? AND released=0 AND attempt_id IS ? AND attempt_number IS ?", [grant.kind, grant.subjectID, grant.nodeID, grant.workerPrincipalID, grant.incarnation, grant.epoch, grant.expiresAt, now, grant.attemptID, grant.attemptNumber])],
      evidence: value(grant), result: null, validBefore: grant.expiresAt });
  }

  /** Include all guards and validBefore: grant.expiresAt in the same business commit. */
  guard(grant: LeaseGrant, queuedRecheck = false): SQLWrite[] {
    this.checkGrant(grant);
    const now = this.now();
    const checks: SQLWrite[] = [
      write("UPDATE business_leases SET expires_at=expires_at+0 WHERE kind=? AND subject_id=? AND node_id=? AND worker_principal_id=? AND incarnation=? AND epoch=? AND expires_at>=? AND expires_at>? AND released=0 AND attempt_id IS ? AND attempt_number IS ?",
        [grant.kind, grant.subjectID, grant.nodeID, grant.workerPrincipalID, grant.incarnation, grant.epoch, grant.expiresAt, now, grant.attemptID, grant.attemptNumber]),
      // The business deadline must fit inside the currently confirmed session.
      write("UPDATE node_sessions SET expires_at=expires_at+0 WHERE incarnation=? AND node_id=? AND principal_id=? AND state='online' AND expires_at>=?",
        [grant.incarnation, grant.nodeID, grant.workerPrincipalID, grant.expiresAt]),
    ];
    const recheck = grant.kind === "action" && (queuedRecheck || this.isRecheck(grant));
    if (recheck) {
      if (this.authorizeRecheck) checks.push(this.authorizeRecheck(grant.subjectID));
      // Only a durable management queue or an immutable current recheck Attempt
      // bypasses business cancellation; this authority cannot grant execute.
      if (queuedRecheck) {
        const work = this.store.query("SELECT id,stage FROM work_items WHERE kind='action' AND subject_id=?", [grant.subjectID])[0];
        const action = this.store.query("SELECT revision,snapshot_json FROM actions WHERE id=?", [grant.subjectID])[0];
        const control = action && readActionControl(this.store, grant.subjectID);
        if (!work || !action || !control || control.revision !== action.revision || !(work.stage === "recheck" && control.recheckRequested || work.stage === "running" && control.latestAttempt?.mode === "recheck-only")) throw harnessError("EXECUTION_FENCED", "Recheck queue is no longer current");
        checks.push(write("UPDATE work_items SET stage=? WHERE id=? AND kind=? AND subject_id=? AND stage=?", [work.stage as string, work.id as string, "action", grant.subjectID, work.stage as string]),
          write("UPDATE actions SET epoch=epoch+0 WHERE id=? AND epoch=? AND snapshot_json=?", [grant.subjectID, grant.epoch, action.snapshot_json as string]));
      } else checks.push(write("UPDATE action_attempt_state SET stage=? WHERE action_id=? AND attempt_id=? AND mode=? AND stage=? AND execute_dispatched=0", ["reconcile", grant.subjectID, grant.attemptID!, "recheck-only", "reconcile"]),
        write("UPDATE actions SET epoch=epoch+0 WHERE id=? AND epoch=?", [grant.subjectID, grant.epoch]));
      return checks;
    }
    if (grant.kind === "run") {
      checks.push(write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND epoch=? AND business_cancelled=0 AND business_terminal=0", [grant.subjectID, grant.epoch]));
    } else {
      const action = this.store.query("SELECT run_id FROM actions WHERE id=?", [grant.subjectID])[0];
      if (!action) throw harnessError("EXECUTION_FENCED", "Action no longer exists");
      checks.push(write("UPDATE actions SET epoch=epoch+0 WHERE id=? AND run_id=? AND epoch=? AND business_cancelled=0 AND business_terminal=0 AND outcome_json IS NULL", [grant.subjectID, action.run_id as string, grant.epoch]),
        write("UPDATE runs SET epoch=epoch+0 WHERE id=? AND business_cancelled=0 AND business_terminal=0", [action.run_id as string]));
    }
    // The ancestor cancellation barrier is evaluated under the same write lock
    // as the business mutation, independently of asynchronous cleanup.
    checks.push(cancellationGuard({ kind: grant.kind, id: grant.subjectID }));
    return checks;
  }

  assert(grant: LeaseGrant): void {
    this.checkGrant(grant);
    const now = this.now();
    const row = this.store.query("SELECT node_id,worker_principal_id,incarnation,epoch,expires_at,released,attempt_id,attempt_number FROM business_leases WHERE kind=? AND subject_id=?", [grant.kind, grant.subjectID])[0];
    const session = this.session(grant, now);
    const subject = grant.kind === "run"
      ? this.store.query("SELECT epoch,business_cancelled,business_terminal FROM runs WHERE id=?", [grant.subjectID])[0]
      : this.store.query("SELECT a.epoch,a.business_cancelled,a.business_terminal,a.outcome_json,r.business_cancelled run_cancelled,r.business_terminal run_terminal FROM actions a JOIN runs r ON r.id=a.run_id WHERE a.id=?", [grant.subjectID])[0];
    const recheck = grant.kind === "action" && this.isRecheck(grant);
    if (recheck && this.authorizeRecheck) this.authorizeRecheck(grant.subjectID);
    if (!row || !subject || row.node_id !== grant.nodeID || row.worker_principal_id !== grant.workerPrincipalID || row.incarnation !== grant.incarnation || row.epoch !== grant.epoch || (row.expires_at as number) < grant.expiresAt || row.released !== 0 || row.attempt_id !== grant.attemptID || row.attempt_number !== grant.attemptNumber || grant.expiresAt <= now || subject.epoch !== grant.epoch || (session.expires_at as number) < grant.expiresAt || !recheck && (subject.business_cancelled !== 0 || subject.business_terminal !== 0 || grant.kind === "action" && (subject.outcome_json !== null || subject.run_cancelled !== 0 || subject.run_terminal !== 0)))
      throw harnessError("EXECUTION_FENCED", "Business lease is no longer current");
    if (!recheck && this.store.effectiveCancellation({ kind: grant.kind, id: grant.subjectID }))
      throw harnessError("EXECUTION_FENCED", "Business lease was cancelled");
  }

  private isRecheck(grant: LeaseGrant): boolean {
    if (grant.kind !== "action" || !grant.attemptID) return false;
    return !!this.store.query("SELECT 1 FROM action_attempt_state s JOIN work_items w ON w.kind='action' AND w.subject_id=s.action_id WHERE s.action_id=? AND s.attempt_id=? AND s.mode='recheck-only' AND s.stage='reconcile' AND s.execute_dispatched=0 AND w.stage IN ('running','report_pending')", [grant.subjectID, grant.attemptID])[0];
  }

  private session(identity: { nodeID: string; workerPrincipalID: string; incarnation: string }, now: number): Row {
    const row = this.store.query("SELECT expires_at FROM node_sessions WHERE incarnation=? AND node_id=? AND principal_id=? AND state='online' AND expires_at>?", [identity.incarnation, identity.nodeID, identity.workerPrincipalID, now])[0];
    if (!row) throw harnessError("EXECUTION_FENCED", "Node session is no longer current");
    return row;
  }
  private sessionGuard(identity: { nodeID: string; workerPrincipalID: string; incarnation: string }, session: Row, now: number): SQLWrite {
    return write("UPDATE node_sessions SET expires_at=expires_at+0 WHERE incarnation=? AND node_id=? AND principal_id=? AND state='online' AND expires_at=? AND expires_at>?", [identity.incarnation, identity.nodeID, identity.workerPrincipalID, session.expires_at as number, now]);
  }
  private now(): number { const now = this.clock.now(); if (!Number.isSafeInteger(now) || now < 0) throw harnessError("INVALID_ARGUMENT", "Invalid clock"); return now; }
  private checkRequest(r: ClaimRequest): void {
    if (!r || !["run", "action"].includes(r.kind) || !id(r.subjectID) || !id(r.nodeID) || !id(r.workerPrincipalID) || !id(r.incarnation) || !id(r.claimRequestID)) throw harnessError("INVALID_ARGUMENT", "Invalid claim");
  }
  private checkGrant(g: LeaseGrant): void {
    if (!g || !["run", "action"].includes(g.kind) || !id(g.subjectID) || !id(g.nodeID) || !id(g.workerPrincipalID) || !id(g.incarnation) || !Number.isSafeInteger(g.epoch) || g.epoch < 1 || !Number.isSafeInteger(g.expiresAt) || g.expiresAt < 0 || (g.kind === "run" ? g.attemptID !== null || g.attemptNumber !== null : !id(g.attemptID) || !Number.isSafeInteger(g.attemptNumber) || g.attemptNumber! < 1)) throw harnessError("INVALID_ARGUMENT", "Invalid business grant");
  }
  private replay(operationID: string, kind: string, subjectID: string, request: unknown): { found: boolean; result: Json } {
    const row = this.store.query("SELECT type,subject_id,request_json,result_json FROM history WHERE operation_id=?", [operationID])[0];
    if (!row) return { found: false, result: null };
    if (row.type !== kind || row.subject_id !== subjectID || row.request_json !== canonicalJson(request)) throw harnessError("REQUEST_CONFLICT", "Business lease operation identity was reused");
    return { found: true, result: parseJson(row.result_json as string) };
  }
}

