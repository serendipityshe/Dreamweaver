import { randomUUID } from "node:crypto";
import { DatabaseSync, type SQLInputValue, type SQLOutputValue } from "node:sqlite";
import { harnessError, HarnessError, canonicalJson, canonicalJsonDigest, copyJson, parseJson, type Json } from "../contract/index.js";
import { migrate, schemaVersion } from "./migrations.js";
import { compileWrite, type SQLWrite } from "./writes.js";
import { CancellationResolver, type CancellationSubject } from "./cancellation.js";
import { assertNotRestoredSnapshot } from "./restore-quarantine.js";
export type { SQLWrite } from "./writes.js";

/** Narrow negative predicates checked under the same lock as the decision. */
export type AbsenceGuard =
  | { table: "version_pointers"; kind: "agent" | "capacity"; id: string }
  | { table: "definition_catalog"; kind: "agent" | "capacity" | "policy"; id: string; version: string }
  | { table: "runs" | "actions"; id: string }
  | { table: "claim_receipt"; id: string }
  | { table: "value_reference"; id: string }
  | { table: "capacity_reconciliation_support"; id: string; version: string }
  | { table: "online_definition"; kind: "agent" | "capacity"; id: string; version: string; at: number };
export interface HistoryEvent { type: string; subjectID: string; data: Json; dataProjectionID?: string }
export interface ValuePublication { id: string; owner: string; encoding: string; length: number; digest: string }
export interface CommitBatch {
  operationID: string;
  kind: string;
  subjectID: string;
  /** Complete semantic identity, including accepted outcome/evidence when relevant. Excludes generated SQL IDs/timestamps. */
  request: Json;
  writes: SQLWrite[];
  /** Conditions are checked under the same write lock as the batch, only for a new operation. */
  absent?: AbsenceGuard[];
  /** Exact bounded fact set read before a wait decision; checked under the write lock. */
  factSnapshot?: { actionID: string; factID: string | null }[];
  /** Exact immutable facts fixed in a frame input, checked before that frame is consumed. */
  frameFactSnapshot?: { frameID: string; factIDs: string[] };
  /** Append-only domain history, committed with the operation proof; include its meaning in request. */
  events?: HistoryEvent[];
  /** Complete immutable values to publish with this business operation. */
  valuePublications?: ValuePublication[];
  evidence: Json;
  result: Json;
  /** Private absolute deadline for a new atomic set, checked after acquiring the write lock. Never extends a grant. */
  validBefore?: number;
}
export interface CommitEvidence {
  operationID: string;
  sequence: number;
  kind: string;
  subjectID: string;
  at: number;
  evidence: Json;
  result: Json;
}
export interface StorageOptions {
  busyTimeoutMs?: number;
  clock?: { now(): number };
  readOnly?: boolean;
  /** Prepare/migrate without taking authority. Only claim after deployment prerequisites are ready. */
  deferAuthority?: boolean;
  /** Trusted extra connection of the SAME coordinator, never a worker-node credential. Omit for a new authority. */
  coordinatorEpoch?: number;
}

/** Coordinator-private SQLite implementation. No callbacks execute while holding a transaction. */
export class SQLiteStore {
  #db: DatabaseSync;
  readonly #clock: { now(): number };
  readonly #path: string;
  readonly #busyTimeout: number;
  readonly #domainID: string;
  #epoch = 0;
  readonly #readOnly: boolean;
  #uncertain = false;
  #closed = false;
  #cancellation: CancellationResolver;
  #oldConnectionClosed = false;
  #closeUncertain = false;
  #recovery: { oldEpoch: number; newEpoch: number } | undefined;
  #witness: DatabaseSync | undefined;
  #witnessSerial = 0;
  #armedWitness: { id: number; version: number } | undefined;
  #recoveredWitness: { id: number; unchanged: boolean } | undefined;
  #unresolvedCommit: CommitBatch | undefined;

  constructor(path: string, options: StorageOptions = {}) {
    const busyTimeout = options.busyTimeoutMs ?? 1_000;
    if (!Number.isSafeInteger(busyTimeout) || busyTimeout < 0 || busyTimeout > 60_000 || typeof path !== "string" || !path || path === ":memory:") {
      throw harnessError("INVALID_ARGUMENT", "A file database and a finite busy timeout in 0..60000 are required");
    }
    // Check before opening, migration, authority claim, or any scheduler exists.
    // Read-only observers remain available for offline evidence inspection.
    if (!options.readOnly) assertNotRestoredSnapshot(path);
    this.#clock = options.clock ?? { now: Date.now };
    this.#path = path;
    this.#busyTimeout = busyTimeout;
    this.#readOnly = options.readOnly ?? false;
    const deferred = options.deferAuthority ?? false;
    if (typeof deferred !== "boolean" || deferred && (this.#readOnly || options.coordinatorEpoch !== undefined)) {
      throw harnessError("INVALID_ARGUMENT", "Deferred authority cannot be combined with observer or attached connections");
    }
    let opened: DatabaseSync | undefined;
    try {
      opened = new DatabaseSync(path, { enableForeignKeyConstraints: true, allowExtension: false, readOnly: this.#readOnly });
      this.#db = opened;
      this.#db.exec(`PRAGMA busy_timeout=${busyTimeout}; PRAGMA trusted_schema=OFF`);
      if (!this.#readOnly) this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL");
      if (this.#db.prepare("PRAGMA journal_mode").get()!.journal_mode !== "wal") throw new Error("WAL unavailable");
      if (this.#readOnly || options.coordinatorEpoch !== undefined) {
        if (this.#db.prepare("PRAGMA user_version").get()!.user_version !== schemaVersion) throw new Error("Observer requires current schema");
        this.#epoch = this.#db.prepare("SELECT coordinator_epoch FROM domain WHERE singleton=1").get()!.coordinator_epoch as number;
        if (options.coordinatorEpoch !== undefined && (this.#readOnly || options.coordinatorEpoch !== this.#epoch)) throw new Error("Invalid coordinator grant");
      } else {
        migrate(this.#db);
        if (!deferred) this.claimAuthority();
      }
      if (!Number.isSafeInteger(this.#epoch) || this.#epoch < 0 || this.#epoch === 0 && !deferred && !this.#readOnly) throw new Error("Invalid coordinator epoch");
      const domainID = this.#db.prepare("SELECT domain_id FROM domain WHERE singleton=1").get()?.domain_id;
      if (!validID(domainID)) throw new Error("Invalid domain identity");
      this.#domainID = domainID;
      this.#cancellation = new CancellationResolver(this.#db);
    } catch (error) {
      try { opened?.close(); } catch { /* Preserve the initialization failure. */ }
      if (error instanceof HarnessError && error.info.disposition === "uncertain") throw error;
      throw harnessError("INTERNAL_ERROR", "Harness database initialization failed");
    }
  }

  get domainID(): string {
    this.#assertOpen();
    return this.#read("SELECT domain_id FROM domain WHERE singleton=1")[0]!.domain_id as string;
  }

  get coordinatorEpoch(): number { return this.#epoch; }
  get requiresRecovery(): boolean { return this.#uncertain; }
  /** Original immutable semantic request of a locally uncertain commit. */
  get unresolvedCommit(): CommitBatch | undefined {
    return this.#unresolvedCommit ? copyJson(this.#unresolvedCommit) as unknown as CommitBatch : undefined;
  }

  /** Host-only observation. Never reads through an active/unknown connection or authorizes work. */
  inspectRecovery() {
    let authority: "current" | "superseded" | "unavailable" | "closed" = this.#closed ? "closed" : "unavailable";
    if (!this.#closed) {
      try {
        const epoch = this.#read("SELECT coordinator_epoch FROM domain WHERE singleton=1")[0]?.coordinator_epoch;
        if (typeof epoch === "number" && this.#epoch > 0) authority = epoch === this.#epoch ? "current" : "superseded";
      } catch { /* Status remains available even when authoritative reads are forbidden. */ }
    }
    const pending = this.#unresolvedCommit;
    return { authority, coordinatorEpoch: this.#epoch, frozen: this.#uncertain,
      pendingOperation: pending ? { operationID: pending.operationID, kind: pending.kind, subjectID: pending.subjectID } : null };
  }

  /** Quarantine the shared connection until the Engine resolves the original operation. */
  freezeForRecovery(): void {
    this.#assertOpen();
    if (this.#readOnly || this.#epoch === 0) throw harnessError("STATE_CONFLICT", "Only an active coordinator can freeze for recovery");
    this.#uncertain = true;
  }

  /** A separate connection witnesses every committed database change while a
   * Core use case is assembled. It never holds a read transaction open. */
  armRecoveryWitness(): number {
    this.assertAuthority();
    if (!this.#witness) {
      const witness = new DatabaseSync(this.#path, { readOnly: true, allowExtension: false });
      try {
        witness.exec(`PRAGMA busy_timeout=${this.#busyTimeout}; PRAGMA trusted_schema=OFF`);
        if (witness.prepare("SELECT domain_id FROM domain WHERE singleton=1").get()?.domain_id !== this.#domainID) throw new Error("Witness domain mismatch");
        this.#witness = witness;
      } catch (error) { try { witness.close(); } catch { /* Preserve the cause. */ } throw error; }
    }
    const version = this.#witness.prepare("PRAGMA data_version").get()?.data_version;
    if (typeof version !== "number" || !Number.isSafeInteger(version)) throw new Error("Witness data version is unavailable");
    const id = ++this.#witnessSerial;
    this.#armedWitness = { id, version };
    return id;
  }

  /** Only meaningful after old-path closure and epoch takeover under the write lock. */
  recoveryWitnessUnchanged(id: number): boolean {
    return !!this.#recovery && this.#recoveredWitness?.id === id && this.#recoveredWitness.unchanged;
  }

  /**
   * Replace a frozen connection without replacing the object held by services.
   * The old connection is closed first; successful acquisition of the new
   * SQLite write lock and CAS epoch proves that the old write transaction has
   * ended. Reads use the new connection but authority and writes stay frozen
   * until the Engine has resolved every sealed original operation.
   */
  recoverAuthority(): { oldEpoch: number; newEpoch: number } {
    this.#assertOpen();
    if (!this.#uncertain || this.#readOnly || this.#epoch === 0) throw harnessError("STATE_CONFLICT", "Coordinator has no frozen authority to recover");
    if (this.#recovery) return { ...this.#recovery };
    const oldEpoch = this.#epoch;
    if (!this.#oldConnectionClosed) {
      this.#closeUncertain = true;
      try { this.#db.close(); this.#oldConnectionClosed = true; this.#closeUncertain = false; }
      catch { throw harnessError("INTERNAL_ERROR", "Old coordinator connection could not be closed", null, "uncertain"); }
    }
    let next: DatabaseSync | undefined;
    try {
      next = new DatabaseSync(this.#path, { enableForeignKeyConstraints: true, allowExtension: false });
      next.exec(`PRAGMA busy_timeout=${this.#busyTimeout}; PRAGMA trusted_schema=OFF; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL`);
      if (next.prepare("PRAGMA user_version").get()?.user_version !== schemaVersion
          || next.prepare("SELECT domain_id FROM domain WHERE singleton=1").get()?.domain_id !== this.#domainID) throw new Error("Recovery database identity or schema changed");
      const cancellation = new CancellationResolver(next);
      // Check the same witness while holding SQLite's write lock, before the
      // epoch update itself changes the witness. No old or concurrent writer
      // can commit between this comparison and our authority fence.
      next.exec("BEGIN IMMEDIATE");
      let unchanged = false;
      if (this.#witness && this.#armedWitness) {
        try { unchanged = this.#witness.prepare("PRAGMA data_version").get()?.data_version === this.#armedWitness.version; }
        catch { /* A failed witness is never proof of unchanged state. */ }
      }
      const result = next.prepare("UPDATE domain SET coordinator_epoch=coordinator_epoch+1 WHERE singleton=1 AND coordinator_epoch=? AND coordinator_epoch<9007199254740991 RETURNING coordinator_epoch").get(oldEpoch);
      const newEpoch = result?.coordinator_epoch;
      if (typeof newEpoch !== "number" || !Number.isSafeInteger(newEpoch) || newEpoch !== oldEpoch + 1) throw new Error("Original coordinator epoch is no longer current");
      next.exec("COMMIT");
      this.#db = next;
      this.#cancellation = cancellation;
      this.#epoch = newEpoch;
      this.#oldConnectionClosed = false;
      this.#recovery = { oldEpoch, newEpoch };
      this.#recoveredWitness = this.#armedWitness ? { id: this.#armedWitness.id, unchanged } : undefined;
      return { ...this.#recovery };
    } catch {
      try { next?.close(); } catch { /* Keep the shared store frozen. */ }
      throw harnessError("INTERNAL_ERROR", "Coordinator recovery could not establish a new authority", null, "uncertain");
    }
  }

  /** Engine may unfreeze only after it has resolved the sealed Core operations. */
  finishRecovery(): void {
    this.#assertOpen();
    if (!this.#uncertain || !this.#recovery) throw harnessError("STATE_CONFLICT", "Coordinator recovery has not reopened its connection");
    if (this.#read("SELECT coordinator_epoch FROM domain WHERE singleton=1")[0]?.coordinator_epoch !== this.#recovery.newEpoch) throw harnessError("EXECUTION_FENCED", "Recovered coordinator authority was superseded");
    this.#recovery = undefined;
    this.#recoveredWitness = undefined;
    this.#armedWitness = undefined;
    this.#unresolvedCommit = undefined;
    this.#uncertain = false;
  }

  /** Current effective cancellation and snapshot share one SQLite read view. */
  cancellationProjection(subject: CancellationSubject) {
    this.#assertOpen();
    if (this.#oldConnectionClosed || this.#closeUncertain) throw harnessError("INTERNAL_ERROR", "Coordinator recovery has no authoritative connection", null, "uncertain");
    if (this.#db.isTransaction) throw harnessError("INTERNAL_ERROR", "Unresolved transaction; reopen before reading authoritative records", null, "uncertain");
    return this.#cancellation.read(subject);
  }

  effectiveCancellation(subject: CancellationSubject) {
    this.#assertOpen();
    if (this.#oldConnectionClosed || this.#closeUncertain) throw harnessError("INTERNAL_ERROR", "Coordinator recovery has no authoritative connection", null, "uncertain");
    if (this.#db.isTransaction) throw harnessError("INTERNAL_ERROR", "Unresolved transaction; reopen before reading authoritative records", null, "uncertain");
    return this.#cancellation.value(subject);
  }

  /** One-way activation; failure must never restore/reuse an older epoch. */
  claimAuthority(): number {
    this.#assertOpen();
    if (this.#uncertain) throw harnessError("INTERNAL_ERROR", "Unresolved coordinator activation", null, "uncertain");
    if (this.#readOnly || this.#epoch !== 0) throw harnessError("STATE_CONFLICT", "Only an unclaimed coordinator can activate");
    try {
      const epoch = this.#db.prepare("UPDATE domain SET coordinator_epoch=coordinator_epoch+1 WHERE singleton=1 AND coordinator_epoch<9007199254740991 RETURNING coordinator_epoch").get()?.coordinator_epoch;
      if (typeof epoch !== "number" || !Number.isSafeInteger(epoch) || epoch < 1) throw new Error("Coordinator epoch exhausted or invalid");
      this.#epoch = epoch;
      return epoch;
    } catch {
      this.#uncertain = true;
      throw harnessError("INTERNAL_ERROR", "Coordinator activation was not confirmed; inspect persisted authority before recovery", null, "uncertain");
    }
  }

  /** RPC admission gate, including handshakes. Observer reads deliberately do not use it. */
  assertAuthority(): void {
    this.#assertOpen();
    if (this.#uncertain) throw harnessError("INTERNAL_ERROR", "Coordinator authority requires recovery", null, "uncertain");
    if (this.#readOnly || this.#epoch === 0
        || this.#read("SELECT coordinator_epoch FROM domain WHERE singleton=1")[0]?.coordinator_epoch !== this.#epoch) {
      throw harnessError("EXECUTION_FENCED", "This coordinator is not the current authority");
    }
  }

  /** Evidence may be inspected on the fenced, reopened recovery connection
   * while writes and ordinary RPCs remain frozen. This never unfreezes it. */
  assertEvidenceReadAuthority(): void {
    this.#assertOpen();
    if (this.#readOnly || this.#epoch === 0 || this.#oldConnectionClosed || this.#closeUncertain
        || this.#uncertain && (!this.#recovery || this.#recovery.newEpoch !== this.#epoch)
        || this.#read("SELECT coordinator_epoch FROM domain WHERE singleton=1")[0]?.coordinator_epoch !== this.#epoch)
      throw harnessError("EXECUTION_FENCED", "Evidence reading authority is unavailable");
  }

  /** Read-only internal query. A missing receipt is not proof that an external operation did not occur. */
  query(sql: string, params: SQLInputValue[] = []): Record<string, SQLOutputValue>[] {
    this.#assertOpen();
    if (!/^\s*SELECT\b/i.test(sql) || sql.includes(";")) throw harnessError("INVALID_ARGUMENT", "Only a single SELECT is allowed");
    return this.#read(sql, params);
  }

  getCommit(operationID: string): CommitEvidence | null {
    this.#assertOpen();
    const row = this.#read("SELECT * FROM history WHERE operation_id=?", [operationID])[0];
    return row ? receipt(row) : null;
  }

  /** Compare the complete original semantic identity after a frozen connection
   * has been closed and authority has been reacquired. No retry is implied. */
  matchesCommitEvidence(batch: CommitBatch): boolean {
    this.#assertOpen();
    const row = this.#read("SELECT type,subject_id,request_json,request_hash,data_json,result_json FROM history WHERE operation_id=?", [batch.operationID])[0];
    if (!row) return false;
    return row.type === batch.kind && row.subject_id === batch.subjectID
      && row.request_json === canonicalJson(batch.request) && row.request_hash === commitFingerprint(batch)
      && row.data_json === canonicalJson(batch.evidence) && row.result_json === canonicalJson(batch.result);
  }

  /** Recheck private conditional writes under the coordinator write lock,
   * then roll them back. This is used for read-only acknowledgements of an
   * already durable chunk; it must not create a new replay receipt. */
  checkGuards(guards: SQLWrite[]): void {
    this.#assertOpen();
    if (this.#uncertain) throw harnessError("INTERNAL_ERROR", "Reopen the database to resolve the original commit", null, "uncertain");
    if (this.#readOnly || this.#epoch === 0) throw harnessError("EXECUTION_FENCED", "Coordinator authority is unavailable");
    if (!Array.isArray(guards) || !guards.length) throw harnessError("INVALID_ARGUMENT", "Guard conditions are required");
    const compiled = guards.map(guard => {
      if (guard?.sql === "CHECK_EFFECTIVE_CANCELLATION") {
        if (guard.expectChanges !== 1 || !Array.isArray(guard.params) || guard.params.length !== 2 || !["run", "action"].includes(guard.params[0] as string) || !validID(guard.params[1])) throw harnessError("INVALID_ARGUMENT", "Invalid effective cancellation guard");
        return { subject: { kind: guard.params[0] as "run" | "action", id: guard.params[1] as string } };
      }
      return { write: compileWrite(this.#db, guard) };
    });
    try {
      this.#db.exec("BEGIN IMMEDIATE");
      if (this.#db.prepare("UPDATE domain SET coordinator_epoch=coordinator_epoch WHERE singleton=1 AND coordinator_epoch=?").run(this.#epoch).changes !== 1)
        throw harnessError("EXECUTION_FENCED", "Coordinator was fenced by a newer incarnation");
      for (const guard of compiled) {
        if (guard.subject) {
          const cancellation = this.#cancellation.lookup(guard.subject);
          if (cancellation.invalid || cancellation.cancellationJSON !== null) throw harnessError("EXECUTION_FENCED", "Effective cancellation blocks this operation");
        } else if (guard.write!.statement.run(...guard.write!.params).changes !== guard.write!.changes)
          throw harnessError("REVISION_CONFLICT", "Atomic guard condition failed");
      }
      this.#db.exec("ROLLBACK");
    } catch (error) {
      try { if (this.#db.isTransaction) this.#db.exec("ROLLBACK"); }
      catch {
        this.#uncertain = true;
        throw harnessError("INTERNAL_ERROR", "Guard rollback requires coordinator recovery", null, "uncertain");
      }
      if (error instanceof HarnessError) throw error;
      throw harnessError("INTERNAL_ERROR", "Guard check failed");
    }
  }

  /**
   * A batch is copied, canonicalized and compiled before BEGIN IMMEDIATE. The
   * critical section only runs prepared SQL, checks row counts, and records proof.
   * Network/plugin work belongs before this method, followed by SQL conditions.
   */
  commit(input: CommitBatch): CommitEvidence {
    this.#assertOpen();
    if (this.#uncertain) throw harnessError("INTERNAL_ERROR", "Reopen the database to resolve the original commit", null, "uncertain");
    if (this.#readOnly) throw harnessError("STATE_CONFLICT", "Observer connections cannot commit");
    if (this.#epoch === 0) throw harnessError("EXECUTION_FENCED", "Coordinator has not activated");
    // Seal the restricted JSON tree without joining the entire transaction.
    // Actual stored cells are encoded separately below; repeated values in
    // independent writes must not impose a single-string limit on the batch.
    const batch = copyJson(input) as unknown as CommitBatch;
    if (!batch || typeof batch !== "object" || !validID(batch.operationID) || !validID(batch.subjectID) || !validID(batch.kind)
        || !Array.isArray(batch.writes) || !("request" in batch) || !("result" in batch) || !("evidence" in batch)
        || batch.validBefore !== undefined && (!Number.isSafeInteger(batch.validBefore) || batch.validBefore < 0)) {
      throw harnessError("INVALID_ARGUMENT", "Invalid atomic batch");
    }
    const canonical = canonicalJson(batch.request);
    if (batch.valuePublications !== undefined && (!Array.isArray(batch.valuePublications)
      || batch.valuePublications.some(value => !value || Object.keys(value).sort().join(",") !== "digest,encoding,id,length,owner"
        || !validID(value.id) || !validID(value.owner) || value.encoding !== "utf8-v1"
        || !Number.isSafeInteger(value.length) || value.length <= 0 || !/^[a-f0-9]{64}$/.test(value.digest))
      || new Set(batch.valuePublications.map(value => value.id)).size !== batch.valuePublications.length)) {
      throw harnessError("INVALID_ARGUMENT", "Invalid value publication");
    }
    // Only the dispatch handoff with explicit staged request sources opts in.
    // Its Engine rearms the whole-database witness after staging and verifies
    // each original publication during Core receipt recovery.
    if (batch.valuePublications?.length && (batch.kind.startsWith("core.") || batch.kind.startsWith("cancel.control_"))) {
      const request = batch.request as Record<string, Json>;
      const entries = request?.actionRequestValues as unknown as { key: string; source: { frameID: string; roundID: string; generation: number; value: ValuePublication } }[];
      if (batch.kind !== "core.suspend" || !request || typeof request !== "object" || Array.isArray(request) || request.encoding !== "core-dispatch-ref-v1" || !Array.isArray(entries)
          || entries.some(item => !item?.source || item.source.frameID !== request.frameID || item.source.roundID !== request.roundID || item.source.generation !== request.generation)
          || canonicalJsonDigest(entries.map(item => item.source.value)) !== canonicalJsonDigest(batch.valuePublications))
        throw harnessError("INVALID_ARGUMENT", "Value publications are unavailable for this Core/control operation");
    }
    const fingerprint = commitFingerprint(batch);
    let statements: ({ kind: "write"; compiled: ReturnType<typeof compileWrite> } | { kind: "cancellation"; subject: CancellationSubject } | { kind: "resolved-effect"; actionID: string; attemptID: string }
      | { kind: "reconciled-effect"; actionID: string; reports: { attemptID: string; reportID: string; reportJSON: string }[];
        marker: [string, string, string] | null; countCheck: ReturnType<DatabaseSync["prepare"]>; reportCheck: ReturnType<DatabaseSync["prepare"]>; markerCheck: ReturnType<DatabaseSync["prepare"]> })[];
    let absenceChecks: { statement: ReturnType<DatabaseSync["prepare"]>; params: (string | number)[] }[];
    let factChecks: { statement: ReturnType<DatabaseSync["prepare"]>; actionID: string; factID: string | null }[];
    let frameFactCheck: { statement: ReturnType<DatabaseSync["prepare"]>; frameID: string; factIDs: string[] } | undefined;
    try {
      statements = batch.writes.map(write => {
        if (write && write.sql === "CHECK_ACTION_EFFECT_RECONCILED") {
          if (write.expectChanges !== 1 || !Array.isArray(write.params) || write.params.length < 7 || write.params.length % 3 !== 1 || !validID(write.params[0])) throw harnessError("INVALID_ARGUMENT", "Invalid reconciled effect guard");
          const marker = write.params.slice(1, 4);
          if (!marker.every(value => value === null) && !(validID(marker[0]) && typeof marker[1] === "string" && typeof marker[2] === "string")) throw harnessError("INVALID_ARGUMENT", "Invalid reconciled effect marker");
          const reports: { attemptID: string; reportID: string; reportJSON: string }[] = [];
          for (let index = 4; index < write.params.length; index += 3) {
            const [attemptID, reportID, reportJSON] = write.params.slice(index, index + 3);
            if (!validID(attemptID) || !validID(reportID) || typeof reportJSON !== "string") throw harnessError("INVALID_ARGUMENT", "Invalid reconciled effect proof");
            reports.push({ attemptID: attemptID as string, reportID: reportID as string, reportJSON });
          }
          if (new Set(reports.map(report => report.attemptID)).size !== reports.length) throw harnessError("INVALID_ARGUMENT", "Duplicate reconciled effect proof");
          return { kind: "reconciled-effect" as const, actionID: write.params[0] as string, reports, marker: marker[0] === null ? null : marker as [string, string, string],
            countCheck: this.#db.prepare(`SELECT
              (SELECT COUNT(*) FROM action_attempt_state WHERE action_id=? AND execute_dispatched=1) attempts,
              (SELECT COUNT(*) FROM action_reports WHERE action_id=? AND phase='execute') reports,
              (SELECT COUNT(*) FROM history WHERE subject_id=? AND type='action.report_conflict') conflicts`),
            reportCheck: this.#db.prepare(`SELECT 1 FROM action_attempt_state s JOIN action_reports r ON r.attempt_id=s.attempt_id AND r.action_id=s.action_id
              WHERE s.action_id=?1 AND s.attempt_id=?2 AND s.execute_dispatched=1 AND r.id=?3 AND r.phase='execute'
                AND ((substr(?4,1,4)<>'ref:' AND r.report_value_id IS NULL AND r.report_json=?4)
                  OR (substr(?4,1,4)='ref:' AND r.report_json='null' AND r.report_value_id=json_extract(substr(?4,5),'$.id')
                    AND EXISTS (SELECT 1 FROM value_manifests m JOIN value_references v ON v.value_id=m.id JOIN history h ON h.operation_id=v.operation_id
                      WHERE m.id=r.report_value_id AND m.state='published' AND m.owner=json_extract(substr(?4,5),'$.owner')
                        AND m.encoding=json_extract(substr(?4,5),'$.encoding') AND m.total_bytes=json_extract(substr(?4,5),'$.length')
                        AND m.written_bytes=m.total_bytes AND m.digest=json_extract(substr(?4,5),'$.digest')
                        AND h.type='action.report_received' AND h.subject_id=r.action_id AND json_extract(h.request_json,'$.actionID')=r.action_id
                        AND json_extract(h.request_json,'$.attemptID')=r.attempt_id AND json_extract(h.request_json,'$.phase')=r.phase AND json_extract(h.request_json,'$.reportID')=r.id
                        AND json_extract(h.request_json,'$.reportValue')=json(substr(?4,5)))))`),
            markerCheck: this.#db.prepare(`SELECT 1 FROM action_reports r JOIN action_observations o ON o.id=r.id AND o.action_id=r.action_id JOIN report_processing p ON p.report_id=r.id
              WHERE r.action_id=?1 AND r.id=?2
                AND ((substr(?4,1,11)='projection:' AND o.snapshot_json='null' AND o.snapshot_projection_id=substr(?4,12))
                  OR (substr(?4,1,11)<>'projection:' AND o.snapshot_projection_id IS NULL AND o.snapshot_json=?4))
                AND r.phase='reconcile' AND r.authority_json IS NOT NULL AND p.state='done'
                AND ((substr(?3,1,4)<>'ref:' AND r.report_value_id IS NULL AND r.report_json=?3)
                  OR (substr(?3,1,4)='ref:' AND r.report_json='null' AND r.report_value_id=json_extract(substr(?3,5),'$.id')
                    AND EXISTS (SELECT 1 FROM value_manifests m JOIN value_references v ON v.value_id=m.id JOIN history h ON h.operation_id=v.operation_id
                      WHERE m.id=r.report_value_id AND m.state='published' AND m.owner=json_extract(substr(?3,5),'$.owner')
                        AND m.encoding=json_extract(substr(?3,5),'$.encoding') AND m.total_bytes=json_extract(substr(?3,5),'$.length')
                        AND m.written_bytes=m.total_bytes AND m.digest=json_extract(substr(?3,5),'$.digest')
                        AND h.type='action.report_received' AND h.subject_id=r.action_id AND json_extract(h.request_json,'$.actionID')=r.action_id
                        AND json_extract(h.request_json,'$.attemptID')=r.attempt_id AND json_extract(h.request_json,'$.phase')=r.phase AND json_extract(h.request_json,'$.reportID')=r.id
                        AND json_extract(h.request_json,'$.reportValue')=json(substr(?3,5)))))`) };
        }
        if (write && write.sql === "CHECK_ACTION_EFFECT_RESOLVED") {
          if (write.expectChanges !== 1 || !Array.isArray(write.params) || write.params.length !== 2 || !validID(write.params[0]) || !validID(write.params[1])) throw harnessError("INVALID_ARGUMENT", "Invalid resolved effect guard");
          return { kind: "resolved-effect" as const, actionID: write.params[0] as string, attemptID: write.params[1] as string };
        }
        if (write && write.sql === "CHECK_EFFECTIVE_CANCELLATION") {
          if (write.expectChanges !== 1 || !Array.isArray(write.params) || write.params.length !== 2 || !["run", "action"].includes(write.params[0] as string) || !validID(write.params[1])) throw harnessError("INVALID_ARGUMENT", "Invalid effective cancellation guard");
          return { kind: "cancellation" as const, subject: { kind: write.params[0] as "run" | "action", id: write.params[1] as string } };
        }
        return { kind: "write" as const, compiled: compileWrite(this.#db, write) };
      });
      if (batch.absent !== undefined && !Array.isArray(batch.absent)) throw harnessError("INVALID_ARGUMENT", "Invalid absence conditions");
      absenceChecks = (batch.absent ?? []).map(guard => {
        if (!guard || !validID(guard.id)) throw harnessError("INVALID_ARGUMENT", "Invalid absence condition");
        if (guard.table === "claim_receipt" && guard.id.startsWith("node.claim:") && Object.keys(guard).length === 2) {
          return { statement: this.#db.prepare("SELECT 1 FROM history WHERE operation_id=?"), params: [guard.id] };
        }
        if (guard.table === "value_reference" && Object.keys(guard).length === 2) {
          return { statement: this.#db.prepare("SELECT 1 FROM value_references WHERE value_id=?"), params: [guard.id] };
        }
        if ((guard.table === "runs" || guard.table === "actions") && Object.keys(guard).length === 2) {
          return { statement: this.#db.prepare(`SELECT 1 FROM ${guard.table} WHERE id=?`), params: [guard.id] };
        }
        if (guard.table === "capacity_reconciliation_support" && validID(guard.version) && Object.keys(guard).length === 3) {
          return { statement: this.#db.prepare("SELECT 1 FROM capacity_reconciliation_support WHERE id=? AND version=?"), params: [guard.id, guard.version] };
        }
        if (!("kind" in guard) || !["agent", "capacity", "policy"].includes(guard.kind)) throw harnessError("INVALID_ARGUMENT", "Invalid absence condition");
        if (guard.table === "online_definition" && ["agent", "capacity"].includes(guard.kind) && validID(guard.version) && Number.isSafeInteger(guard.at) && guard.at >= 0 && Object.keys(guard).length === 5) {
          return { statement: this.#db.prepare("SELECT 1 FROM node_definitions d JOIN node_sessions s ON s.incarnation=d.incarnation WHERE d.kind=? AND d.id=? AND d.version=? AND s.state='online' AND s.expires_at>? LIMIT 1"), params: [guard.kind, guard.id, guard.version, guard.at] };
        }
        if (guard.table === "version_pointers" && ["agent", "capacity"].includes(guard.kind) && Object.keys(guard).length === 3) {
          return { statement: this.#db.prepare("SELECT 1 FROM version_pointers WHERE kind=? AND id=?"), params: [guard.kind, guard.id] };
        }
        if (guard.table === "definition_catalog" && validID(guard.version) && Object.keys(guard).length === 4) {
          return { statement: this.#db.prepare("SELECT 1 FROM definition_catalog WHERE kind=? AND id=? AND version=?"), params: [guard.kind, guard.id, guard.version] };
        }
        throw harnessError("INVALID_ARGUMENT", "Invalid absence condition");
      });
      if (batch.factSnapshot !== undefined && !Array.isArray(batch.factSnapshot)) throw harnessError("INVALID_ARGUMENT", "Invalid fact snapshot");
      const seenFacts = new Set<string>();
      factChecks = (batch.factSnapshot ?? []).map(fact => {
        if (!fact || Object.keys(fact).length !== 2 || !validID(fact.actionID) || fact.factID !== null && !validID(fact.factID) || seenFacts.has(fact.actionID)) throw harnessError("INVALID_ARGUMENT", "Invalid fact snapshot");
        seenFacts.add(fact.actionID);
        return { statement: this.#db.prepare("SELECT id FROM facts WHERE action_id=?"), actionID: fact.actionID, factID: fact.factID };
      });
      if (batch.frameFactSnapshot !== undefined) {
        const fixed = batch.frameFactSnapshot;
        if (!fixed || Object.keys(fixed).length !== 2 || !validID(fixed.frameID) || !Array.isArray(fixed.factIDs) || fixed.factIDs.some(id => !validID(id)) || new Set(fixed.factIDs).size !== fixed.factIDs.length) throw harnessError("INVALID_ARGUMENT", "Invalid frame fact snapshot");
        frameFactCheck = { statement: this.#db.prepare("SELECT fact_id FROM frame_facts WHERE frame_id=? ORDER BY fact_id"), frameID: fixed.frameID, factIDs: [...fixed.factIDs].sort() };
      }
    }
    catch (error) {
      if (error instanceof HarnessError) throw error;
      throw harnessError("INTERNAL_ERROR", "Persistence compilation failed before transaction");
    }
    // Clocks, random identity generation and JSON encoding never run under the lock.
    let at: number;
    try { at = this.#clock.now(); }
    catch { throw harnessError("INTERNAL_ERROR", "Coordinator clock failed before transaction"); }
    if (!Number.isSafeInteger(at) || at < 0) throw harnessError("INVALID_ARGUMENT", "Invalid coordinator timestamp");
    const sampledAt = performance.now();
    const historyID = randomUUID();
    const data = canonicalJson(batch.evidence);
    const result = canonicalJson(batch.result);
    if (batch.events !== undefined && (!Array.isArray(batch.events) || batch.events.some(event =>
      !event || !validID(event.type) || !validID(event.subjectID) || !Object.hasOwn(event, "data")
      || Object.keys(event).some(key => !["type", "subjectID", "data", "dataProjectionID"].includes(key))
      || event.dataProjectionID !== undefined && (!validID(event.dataProjectionID) || event.data !== null)))) {
      throw harnessError("INVALID_ARGUMENT", "Invalid history events");
    }
    const events = (batch.events ?? []).map(event => ({ ...event, id: randomUUID(), dataJSON: canonicalJson(event.data) }));
    let insertEvent: ReturnType<DatabaseSync["prepare"]> | undefined;
    try {
      if (events.length) insertEvent = this.#db.prepare(`INSERT INTO history
        (id, operation_id, type, subject_id, at, request_json, data_json, result_json, request_hash,data_projection_id)
        VALUES (?, NULL, ?, ?, ?, ?, ?, 'null', '', ?)`);
    } catch { throw harnessError("INTERNAL_ERROR", "Persistence history compilation failed before transaction"); }
    let committing = false;
    try {
      this.#db.exec("BEGIN IMMEDIATE");
      if (this.#db.prepare("UPDATE domain SET coordinator_epoch=coordinator_epoch WHERE singleton=1 AND coordinator_epoch=?").run(this.#epoch).changes !== 1) {
        throw harnessError("REVISION_CONFLICT", "Coordinator was fenced by a newer incarnation", { coordinatorEpoch: this.#epoch });
      }
      const existing = this.#db.prepare("SELECT * FROM history WHERE operation_id=?").get(batch.operationID);
      let record: Record<string, SQLOutputValue>;
      if (existing) {
        if (existing.request_hash !== fingerprint || existing.request_json !== canonical || existing.type !== batch.kind || existing.subject_id !== batch.subjectID) throw harnessError("REQUEST_CONFLICT", "Operation identity is bound to different content");
        record = existing;
      } else {
        // The injectable wall clock was sampled before BEGIN. Count write-lock
        // waiting conservatively with the built-in monotonic clock; no host
        // callback or JSON processing runs while holding the lock.
        if (batch.validBefore !== undefined && at + Math.ceil(performance.now() - sampledAt) >= batch.validBefore) {
          throw harnessError("EXECUTION_FENCED", "Atomic operation deadline elapsed before the write lock was acquired");
        }
        for (const value of batch.valuePublications ?? []) {
          const updated = this.#db.prepare("UPDATE value_manifests SET state='published' WHERE id=? AND owner=? AND encoding=? AND total_bytes=? AND digest=? AND state IN ('sealed','published') AND written_bytes=total_bytes")
            .run(value.id, value.owner, value.encoding, value.length, value.digest);
          if (updated.changes !== 1) throw harnessError("REVISION_CONFLICT", "Value is not sealed for publication", { operationID: batch.operationID });
        }
        for (const check of absenceChecks) {
          if (check.statement.get(...check.params)) throw harnessError("REVISION_CONFLICT", "Atomic absence condition failed", { operationID: batch.operationID });
        }
        for (const check of factChecks) {
          const existingFact = check.statement.get(check.actionID);
          if ((existingFact?.id ?? null) !== check.factID) throw harnessError("REVISION_CONFLICT", "Wait facts changed before atomic commit", { operationID: batch.operationID });
        }
        if (frameFactCheck) {
          const actual = frameFactCheck.statement.all(frameFactCheck.frameID);
          if (actual.length !== frameFactCheck.factIDs.length || actual.some((row, index) => row.fact_id !== frameFactCheck!.factIDs[index])) throw harnessError("REVISION_CONFLICT", "Frame facts changed before consumption", { operationID: batch.operationID });
        }
        for (const statement of statements) {
          if (statement.kind === "reconciled-effect") {
            // Recheck the complete immutable execute-report set under the same
            // write lock as clearing blocked. New/unprocessed reports and a
            // conflict cannot be hidden by an earlier optimistic read.
            const counts = statement.countCheck.get(statement.actionID, statement.actionID, statement.actionID)!;
            if (counts.attempts !== statement.reports.length || counts.reports !== statement.reports.length || counts.conflicts !== 0
                || statement.marker && !statement.markerCheck.get(statement.actionID, ...statement.marker)
                || statement.reports.some(report => !statement.reportCheck.get(statement.actionID, report.attemptID, report.reportID, report.reportJSON))) {
              throw harnessError("REVISION_CONFLICT", "External effect proof changed before atomic commit", { operationID: batch.operationID });
            }
            continue;
          }
          if (statement.kind === "resolved-effect") {
            const eligible = this.#db.prepare(`SELECT 1 WHERE
              (SELECT COUNT(*) FROM action_attempt_state WHERE action_id=? AND execute_dispatched=1)=1
              AND (SELECT COUNT(*) FROM action_attempt_state WHERE action_id=?)=1
              AND EXISTS (SELECT 1 FROM action_attempt_state WHERE action_id=? AND attempt_id=? AND execute_dispatched=1)
              AND NOT EXISTS (SELECT 1 FROM history WHERE subject_id=? AND type='action.report_conflict')`).get(statement.actionID, statement.actionID, statement.actionID, statement.attemptID, statement.actionID);
            if (!eligible) throw harnessError("REVISION_CONFLICT", "External effect remains unresolved", { operationID: batch.operationID });
            continue;
          }
          if (statement.kind === "cancellation") {
            const cancellation = this.#cancellation.lookup(statement.subject);
            if (cancellation.invalid || cancellation.cancellationJSON !== null) throw harnessError("EXECUTION_FENCED", "Effective cancellation blocks this operation", { operationID: batch.operationID });
            continue;
          }
          const write = statement.compiled;
          if (write.statement.run(...write.params).changes !== write.changes) {
            throw harnessError("REVISION_CONFLICT", "Atomic write condition failed", { operationID: batch.operationID });
          }
        }
        const inserted = this.#db.prepare(`INSERT INTO history
          (id, operation_id, type, subject_id, at, request_json, data_json, result_json, request_hash)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
          historyID, batch.operationID, batch.kind, batch.subjectID, at, canonical, data, result, fingerprint,
        )!;
        for (const value of batch.valuePublications ?? []) this.#db.prepare("INSERT INTO value_references(value_id,operation_id) VALUES(?,?)").run(value.id, batch.operationID);
        for (const event of events) insertEvent!.run(event.id, event.type, event.subjectID, at, canonical, event.dataJSON, event.dataProjectionID ?? null);
        record = { operation_id: batch.operationID, sequence: inserted.lastInsertRowid, type: batch.kind, subject_id: batch.subjectID, at, data_json: data, result_json: result };
      }
      committing = true;
      this.#db.exec("COMMIT");
      // Decoding a possibly large existing receipt is outside the write lock too.
      return receipt(record);
    } catch (error) {
      let rollbackFailed = false;
      try { if (this.#db.isTransaction) this.#db.exec("ROLLBACK"); }
      catch { rollbackFailed = true; }
      if (committing || rollbackFailed) {
        this.#uncertain = true;
        this.#unresolvedCommit = batch;
        throw harnessError("INTERNAL_ERROR", "SQLite commit outcome requires original identity lookup", { operationID: batch.operationID }, "uncertain");
      }
      if (error instanceof HarnessError) throw error;
      throw harnessError("INTERNAL_ERROR", "SQLite batch was not committed", { operationID: batch.operationID });
    }
  }

  close(): void {
    if (!this.#closed) { if (!this.#oldConnectionClosed) this.#db.close(); this.#witness?.close(); this.#closed = true; }
  }

  #assertOpen(): void {
    if (this.#closed) throw harnessError("INTERNAL_ERROR", "Database is closed");
  }

  #read(sql: string, params: SQLInputValue[] = []): Record<string, SQLOutputValue>[] {
    if (this.#oldConnectionClosed || this.#closeUncertain) throw harnessError("INTERNAL_ERROR", "Coordinator recovery has no authoritative connection", null, "uncertain");
    if (this.#db.isTransaction) throw harnessError("INTERNAL_ERROR", "Unresolved transaction; reopen before reading authoritative records", null, "uncertain");
    try { return this.#db.prepare(sql).all(...params); }
    catch { throw harnessError("INTERNAL_ERROR", "SQLite read failed; no absence can be inferred"); }
  }
}

function commitFingerprint(batch: CommitBatch): string {
  return canonicalJsonDigest({ kind: batch.kind, subjectID: batch.subjectID, request: batch.request,
    ...(batch.valuePublications === undefined ? {} : { valuePublications: batch.valuePublications }),
    ...(batch.events?.some(event => event.dataProjectionID !== undefined) ? { eventProjections: batch.events.map(event => event.dataProjectionID ?? null) } : {}) });
}

function validID(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.trim() === value;
}
function receipt(row: Record<string, SQLOutputValue>): CommitEvidence {
  return { operationID: row.operation_id as string, sequence: row.sequence as number,
    kind: row.type as string, subjectID: row.subject_id as string, at: row.at as number,
    evidence: parseJson(row.data_json as string), result: parseJson(row.result_json as string) };
}
