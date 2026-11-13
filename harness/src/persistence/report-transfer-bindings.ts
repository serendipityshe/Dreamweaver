import { canonicalJson, copyJson, harnessError, parseJson } from "../contract/index.js";
import type { SQLiteStore, ValuePublication } from "./sqlite.js";
import { ValueStore } from "./value-store.js";
import type { SQLWrite } from "./writes.js";

export interface ReportTransferBinding {
  /** Fixed-length digest of the authenticated transfer identity. */
  id: string;
  control: ValuePublication;
  report: ValuePublication;
  /** Digests of the validated, immutable source and worker binding. */
  sourceDigest: string;
  workerDigest: string;
}
export interface DiagnosticTransferReceipt {
  id: string; controlID: string; reportID: string; reportOwner: string; reportLength: number; reportDigest: string;
  sourceDigest: string; workerDigest: string; actionID: string; attemptID: string; phase: "execute" | "reconcile"; actionReportID: string; operationID: string;
}

/** Private persistence seam. The caller must authenticate and validate the
 * source before invoking bind and supply source/session CAS guards. Binding
 * staged data does not grant execution authority or publish a report. */
export class ReportTransferBindings {
  constructor(readonly store: SQLiteStore, readonly values: ValueStore) {}

  lookup(id: string, workerDigest: string): { controlID: string; reportID: string; sourceDigest: string; state: "active" | "published" | "deleting" } {
    if (!/^[a-f0-9]{64}$/.test(id) || !/^[a-f0-9]{64}$/.test(workerDigest)) throw harnessError("INVALID_ARGUMENT", "Invalid report transfer identity");
    this.store.assertAuthority();
    const row = this.store.query("SELECT control_value_id,report_value_id,source_digest,worker_digest,state FROM report_transfer_bindings WHERE id=?", [id])[0];
    if (!row || row.worker_digest !== workerDigest) throw harnessError("FORBIDDEN", "Report transfer is unavailable");
    return { controlID: row.control_value_id as string, reportID: row.report_value_id as string, sourceDigest: row.source_digest as string,
      state: row.state as "active" | "published" | "deleting" };
  }

  bind(input: ReportTransferBinding, guards: SQLWrite[]): void {
    if (!input || !input.control || !input.report || !/^[a-f0-9]{64}$/.test(input.id) || !/^[a-f0-9]{64}$/.test(input.sourceDigest)
        || !/^[a-f0-9]{64}$/.test(input.workerDigest) || !Array.isArray(guards) || !guards.length
        || input.control.id === input.report.id) throw harnessError("INVALID_ARGUMENT", "Invalid report transfer binding");
    this.store.assertAuthority();
    const existing = this.store.query("SELECT * FROM report_transfer_bindings WHERE id=?", [input.id])[0];
    if (existing) {
      if (existing.control_value_id !== input.control.id || existing.report_value_id !== input.report.id
          || existing.source_digest !== input.sourceDigest || existing.worker_digest !== input.workerDigest)
        throw harnessError("REQUEST_CONFLICT", "Report transfer identity is bound to different content");
      if (existing.state !== "active") throw harnessError("STATE_CONFLICT", "Report transfer is not active");
      const control = this.values.status(input.control.id, input.control.owner), report = this.values.status(input.report.id, input.report.owner);
      if (control.state !== "sealed" || control.length !== input.control.length || control.digest !== input.control.digest || control.encoding !== input.control.encoding
          || report.length !== input.report.length || report.digest !== input.report.digest || report.encoding !== input.report.encoding)
        throw harnessError("REQUEST_CONFLICT", "Report transfer content identity changed");
      this.store.checkGuards([...guards, this.chunkGuard(input.id, input.report.id, input.sourceDigest, input.workerDigest)]);
      return;
    }
    const control = this.values.status(input.control.id, input.control.owner);
    if (control.state !== "sealed" || control.length !== input.control.length || control.encoding !== input.control.encoding
        || control.digest !== input.control.digest) throw harnessError("STATE_CONFLICT", "Report transfer control is unavailable");
    const report = this.values.begin(input.report);
    if (report.state !== "staging" || report.written !== 0) throw harnessError("STATE_CONFLICT", "Report transfer must bind before uploading");
    const check = (value: ValuePublication, state: string): SQLWrite => ({
      sql: "UPDATE value_manifests SET revision=revision+0 WHERE id=? AND owner=? AND encoding=? AND total_bytes=? AND digest=? AND state=?",
      params: [value.id, value.owner, value.encoding, value.length, value.digest, state], expectChanges: 1,
    });
    this.store.commit({ operationID: `report.transfer.bind:${input.id}`, kind: "report.transfer_bound", subjectID: input.id,
      request: { id: input.id, controlID: input.control.id, reportID: input.report.id, sourceDigest: input.sourceDigest, workerDigest: input.workerDigest },
      writes: [...guards, check(input.control, "sealed"), check(input.report, "staging"),
        { sql: "INSERT INTO report_transfer_bindings(id,control_value_id,report_value_id,source_digest,worker_digest,state,revision,created_epoch) VALUES(?,?,?,?,?,?,?,?)",
          params: [input.id, input.control.id, input.report.id, input.sourceDigest, input.workerDigest, "active", 0, this.store.coordinatorEpoch], expectChanges: 1 }],
      evidence: null, result: null });
    this.store.checkGuards([...guards, this.chunkGuard(input.id, input.report.id, input.sourceDigest, input.workerDigest)]);
  }

  /** Must be included in the same chunk write transaction, in addition to the
   * coordinator's current source/session/authorization guards. */
  chunkGuard(id: string, reportID: string, sourceDigest: string, workerDigest: string): SQLWrite {
    if (![id, sourceDigest, workerDigest].every(value => typeof value === "string" && /^[a-f0-9]{64}$/.test(value)) || typeof reportID !== "string" || !reportID)
      throw harnessError("INVALID_ARGUMENT", "Invalid report transfer guard");
    return { sql: "UPDATE report_transfer_bindings SET revision=revision+0 WHERE id=? AND report_value_id=? AND source_digest=? AND worker_digest=? AND state=?",
      params: [id, reportID, sourceDigest, workerDigest, "active"], expectChanges: 1 };
  }

  /** Include in the same commit as report publication, business state and
   * receipt, so a concurrent transition to deletion cannot also succeed. */
  publicationWrite(id: string, reportID: string, sourceDigest: string, workerDigest: string): SQLWrite {
    const guard = this.chunkGuard(id, reportID, sourceDigest, workerDigest);
    return { sql: "UPDATE report_transfer_bindings SET state=? WHERE id=? AND report_value_id=? AND source_digest=? AND worker_digest=? AND state=?",
      params: ["published", ...guard.params], expectChanges: 1 };
  }

  /** A committed report retry must still match this binding and current
   * source authorization; it cannot be treated as an active upload. */
  verifyPublished(id: string, reportID: string, sourceDigest: string, workerDigest: string, guards: SQLWrite[]): void {
    if (!Array.isArray(guards) || !guards.length) throw harnessError("INVALID_ARGUMENT", "Report source guards are required");
    const guard = this.chunkGuard(id, reportID, sourceDigest, workerDigest);
    this.store.checkGuards([...guards, { ...guard, params: [...guard.params.slice(0, -1), "published"] }]);
  }

  /** Immutable, self-contained completion evidence survives deletion of the
   * oversized bytes. Control remains available for source reauthorization. */
  diagnostic(id: string, workerDigest: string): DiagnosticTransferReceipt | null {
    if (!/^[a-f0-9]{64}$/.test(id) || !/^[a-f0-9]{64}$/.test(workerDigest)) throw harnessError("INVALID_ARGUMENT", "Invalid report transfer identity");
    this.store.assertAuthority();
    const row = this.store.query("SELECT * FROM report_transfer_diagnostics WHERE id=?", [id])[0];
    if (!row) return null;
    if (row.worker_digest !== workerDigest) throw harnessError("FORBIDDEN", "Report transfer is unavailable");
    if (row.report_value_id !== `report-v4:${id}` || row.report_encoding !== "utf8-v1" || row.digest_scope !== "canonical-report-envelope-utf8-v1" || typeof row.report_owner !== "string" || !row.report_owner
        || !Number.isSafeInteger(row.report_length) || Number(row.report_length) <= 0
        || typeof row.report_digest !== "string" || !/^[a-f0-9]{64}$/.test(row.report_digest)
        || typeof row.source_digest !== "string" || !/^[a-f0-9]{64}$/.test(row.source_digest))
      throw harnessError("INTERNAL_ERROR", "Diagnostic transfer metadata is inconsistent");
    const actionID = row.action_id as string, attemptID = row.attempt_id as string, phase = row.phase as "execute" | "reconcile";
    const history = this.store.query("SELECT request_json FROM history WHERE operation_id=?", [row.operation_id as string])[0];
    const request = history ? parseJson(history.request_json as string) as Record<string, unknown> : null;
    const controlValue = request?.controlValue as ValuePublication | undefined;
    if (!history || !request || !controlValue || !this.store.matchesCommitEvidence({ operationID: row.operation_id as string, kind: "action.report_received", subjectID: actionID,
      request: copyJson(request), writes: [], valuePublications: [controlValue], evidence: { actionID, attemptID, phase }, result: { stored: false, diagnostic: true } }))
      throw harnessError("INTERNAL_ERROR", "Diagnostic transfer receipt is inconsistent");
    const transfer = request.transfer as Record<string, unknown> | undefined;
    const control = this.store.query("SELECT v.id,v.owner,v.encoding,v.total_bytes,v.digest,v.state FROM value_manifests v JOIN value_references r ON r.value_id=v.id WHERE v.id=? AND r.operation_id=?",
      [row.control_value_id as string, row.operation_id as string])[0];
    const report = this.store.query("SELECT action_id,attempt_id,phase,report_json,report_value_id FROM action_reports WHERE id=?", [row.report_id as string])[0];
    const diagnostic = report && typeof report.report_json === "string" ? parseJson(report.report_json) : null;
    if (!transfer || transfer.id !== id || transfer.sourceDigest !== row.source_digest || transfer.workerDigest !== workerDigest
        || request.actionID !== actionID || request.attemptID !== attemptID || request.phase !== phase || request.reportID !== row.report_id
        || request.reportDigest !== row.report_digest || request.reportBytes !== row.report_length || request.reportDigestScope !== row.digest_scope || !control || control.state !== "published"
        || controlValue.id !== control.id || controlValue.owner !== control.owner || controlValue.encoding !== control.encoding
        || controlValue.length !== control.total_bytes || controlValue.digest !== control.digest || controlValue.digest !== row.source_digest
        || !report || report.action_id !== actionID
        || report.attempt_id !== attemptID || report.phase !== phase || report.report_value_id !== null
        || !diagnostic || typeof diagnostic !== "object" || Array.isArray(diagnostic) || diagnostic.kind !== "diagnostic"
        || diagnostic.code !== "RESULT_CONTRACT_VIOLATION" || diagnostic.phase !== phase || diagnostic.digest !== row.report_digest
        || canonicalJson(diagnostic) !== canonicalJson(request.report))
      throw harnessError("INTERNAL_ERROR", "Diagnostic transfer identity is inconsistent");
    return { id, controlID: row.control_value_id as string, reportID: row.report_value_id as string, reportOwner: row.report_owner as string,
      reportLength: row.report_length as number, reportDigest: row.report_digest as string, sourceDigest: row.source_digest as string,
      workerDigest, actionID, attemptID, phase, actionReportID: row.report_id as string, operationID: row.operation_id as string };
  }

  diagnosticControl(transfer: { id: string; sourceDigest: string; workerDigest: string }, reportID: string): ValuePublication {
    const row = this.store.query(`SELECT v.id,v.owner,v.encoding,v.total_bytes,v.digest,v.state FROM report_transfer_bindings b
      JOIN value_manifests v ON v.id=b.control_value_id WHERE b.id=? AND b.report_value_id=? AND b.source_digest=? AND b.worker_digest=? AND b.state='active'`,
      [transfer.id, reportID, transfer.sourceDigest, transfer.workerDigest])[0];
    if (!row || row.state !== "sealed" || row.digest !== transfer.sourceDigest) throw harnessError("STATE_CONFLICT", "Diagnostic control is unavailable");
    return { id: row.id as string, owner: row.owner as string, encoding: row.encoding as string, length: row.total_bytes as number, digest: row.digest as string };
  }

  /** Included with the finite diagnostic, business block and report receipt.
   * The new receipt does not reference the soon-to-be-collected report. */
  diagnosticWrites(transfer: { id: string; sourceDigest: string; workerDigest: string }, ref: ValuePublication,
    actionID: string, attemptID: string, phase: "execute" | "reconcile", reportID: string, operationID: string): SQLWrite[] {
    const binding = this.store.query("SELECT * FROM report_transfer_bindings WHERE id=?", [transfer.id])[0];
    if (!binding || binding.state !== "active" || binding.report_value_id !== ref.id || binding.source_digest !== transfer.sourceDigest
        || binding.worker_digest !== transfer.workerDigest) throw harnessError("STATE_CONFLICT", "Report transfer is unavailable");
    const value = this.values.status(ref.id, ref.owner);
    if (value.state !== "sealed" || value.encoding !== ref.encoding || value.length !== ref.length || value.digest !== ref.digest)
      throw harnessError("STATE_CONFLICT", "Verified report is unavailable");
    return [
      { sql: "UPDATE report_transfer_bindings SET state=? WHERE id=? AND report_value_id=? AND source_digest=? AND worker_digest=? AND state=?",
        params: ["deleting", transfer.id, ref.id, transfer.sourceDigest, transfer.workerDigest, "active"], expectChanges: 1 },
      { sql: "DELETE FROM report_transfer_bindings WHERE id=? AND state=?", params: [transfer.id, "deleting"], expectChanges: 1 },
      { sql: "UPDATE value_manifests SET state=?,revision=? WHERE id=? AND owner=? AND state=? AND revision=? AND total_bytes=? AND digest=?",
        params: ["deleting", value.revision + 1, ref.id, ref.owner, "sealed", value.revision, ref.length, ref.digest], expectChanges: 1 },
      { sql: "INSERT INTO report_transfer_diagnostics(id,control_value_id,report_value_id,report_owner,report_encoding,digest_scope,report_length,report_digest,source_digest,worker_digest,action_id,attempt_id,phase,report_id,operation_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params: [transfer.id, binding.control_value_id as string, ref.id, ref.owner, ref.encoding, "canonical-report-envelope-utf8-v1", ref.length, ref.digest, transfer.sourceDigest, transfer.workerDigest,
          actionID, attemptID, phase, reportID, operationID], expectChanges: 1 },
    ];
  }

  async finishDiagnosticRetirement(receipt: DiagnosticTransferReceipt): Promise<void> {
    this.store.assertAuthority();
    const row = this.store.query("SELECT owner,encoding,total_bytes,digest,state FROM value_manifests WHERE id=?", [receipt.reportID])[0];
    if (row && (row.owner !== receipt.reportOwner || row.encoding !== "utf8-v1" || row.total_bytes !== receipt.reportLength
        || row.digest !== receipt.reportDigest || row.state !== "deleting")) throw harnessError("INTERNAL_ERROR", "Diagnostic report retirement changed");
    await this.values.discard(receipt.reportID, receipt.reportOwner);
  }

  async resumeDiagnosticRetirements(): Promise<number> {
    this.store.assertAuthority(); let cursor = "", completed = 0;
    for (;;) {
      const rows = this.store.query("SELECT id,worker_digest FROM report_transfer_diagnostics WHERE id>? AND EXISTS (SELECT 1 FROM value_manifests v WHERE v.id=report_value_id) ORDER BY id LIMIT 64", [cursor]);
      if (!rows.length) return completed;
      for (const row of rows) {
        const receipt = this.diagnostic(row.id as string, row.worker_digest as string);
        if (!receipt) throw harnessError("INTERNAL_ERROR", "Diagnostic transfer receipt is unavailable");
        await this.finishDiagnosticRetirement(receipt); cursor = receipt.id; completed++;
      }
      if (rows.length < 64) return completed;
    }
  }

  /** Explicitly retire an unpublished transfer after the caller authorizes
   * exclusive access. The atomic transition prevents every subsequent chunk
   * write even if an old in-process uploader continues without its guard. */
  async discard(input: ReportTransferBinding, guards: SQLWrite[]): Promise<void> {
    if (!input || !input.control || !input.report || !Array.isArray(guards) || !guards.length)
      throw harnessError("INVALID_ARGUMENT", "An authorized transfer retirement is required");
    input = copyJson(input) as unknown as ReportTransferBinding;
    this.chunkGuard(input.id, input.report.id, input.sourceDigest, input.workerDigest);
    this.store.assertAuthority();
    const epoch = this.store.coordinatorEpoch, operationID = `report.transfer.discard:${input.id}`;
    const request = { id: input.id, control: input.control, report: input.report, sourceDigest: input.sourceDigest, workerDigest: input.workerDigest };
    const prior = this.store.query("SELECT type,subject_id,request_json FROM history WHERE operation_id=?", [operationID])[0];
    if (prior) {
      if (prior.type !== "report.transfer_discarded" || prior.subject_id !== input.id || prior.request_json !== canonicalJson(request))
        throw harnessError("REQUEST_CONFLICT", "Report transfer retirement identity changed");
      this.store.checkGuards(guards);
    } else {
      const bound = this.store.query("SELECT * FROM report_transfer_bindings WHERE id=?", [input.id])[0];
      if (!bound || bound.control_value_id !== input.control.id || bound.report_value_id !== input.report.id
          || bound.source_digest !== input.sourceDigest || bound.worker_digest !== input.workerDigest || bound.state !== "active")
        throw harnessError("STATE_CONFLICT", "Report transfer cannot be retired");
      const control = this.values.status(input.control.id, input.control.owner), report = this.values.status(input.report.id, input.report.owner);
      for (const [actual, expected] of [[control, input.control], [report, input.report]] as const) {
        if ((actual.state !== "staging" && actual.state !== "sealed") || actual.encoding !== expected.encoding
            || actual.length !== expected.length || actual.digest !== expected.digest)
          throw harnessError("STATE_CONFLICT", "Report transfer content cannot be retired");
      }
      const mark = (value: typeof control): SQLWrite => ({
        sql: "UPDATE value_manifests SET state=?,revision=? WHERE id=? AND owner=? AND state=? AND revision=?",
        params: ["deleting", value.revision + 1, value.id, value.owner, value.state, value.revision], expectChanges: 1,
      });
      this.store.commit({ operationID, kind: "report.transfer_discarded", subjectID: input.id, request: copyJson(request),
        absent: [{ table: "value_reference", id: control.id }, { table: "value_reference", id: report.id }],
        writes: [...guards,
          { sql: "UPDATE report_transfer_bindings SET state=? WHERE id=? AND control_value_id=? AND report_value_id=? AND source_digest=? AND worker_digest=? AND state=?",
            params: ["deleting", input.id, control.id, report.id, input.sourceDigest, input.workerDigest, "active"], expectChanges: 1 },
          mark(control), mark(report),
          { sql: "DELETE FROM report_transfer_bindings WHERE id=? AND state=?", params: [input.id, "deleting"], expectChanges: 1 }],
        evidence: null, result: null });
    }
    await this.finishRetirement(input, epoch);
  }

  /** Resume previously authorized retirements before or during service
   * startup. No TTL or unverified directory scan decides what to delete. */
  async resumeRetirements(): Promise<number> {
    this.store.assertAuthority();
    const epoch = this.store.coordinatorEpoch; let cursor = "", completed = 0;
    for (;;) {
      const rows = this.store.query(`SELECT operation_id,subject_id,request_json FROM history h
        WHERE h.type='report.transfer_discarded' AND h.operation_id>? AND
          (EXISTS (SELECT 1 FROM value_manifests WHERE id=json_extract(h.request_json,'$.control.id'))
           OR EXISTS (SELECT 1 FROM value_manifests WHERE id=json_extract(h.request_json,'$.report.id')))
        ORDER BY h.operation_id LIMIT 64`, [cursor]);
      if (!rows.length) return completed;
      for (const row of rows) {
        const input = parseJson(row.request_json as string) as unknown as ReportTransferBinding;
        if (!input || !input.control || !input.report || row.subject_id !== input.id || row.operation_id !== `report.transfer.discard:${input.id}`)
          throw harnessError("INTERNAL_ERROR", "Report retirement receipt is invalid");
        this.chunkGuard(input.id, input.report.id, input.sourceDigest, input.workerDigest);
        if (this.store.query("SELECT id FROM report_transfer_bindings WHERE id=?", [input.id]).length)
          throw harnessError("INTERNAL_ERROR", "Retired report transfer still has a binding");
        for (const expected of [input.control, input.report]) {
          const row = this.store.query("SELECT owner,encoding,total_bytes,digest,state FROM value_manifests WHERE id=?", [expected.id])[0];
          if (row && (row.owner !== expected.owner || row.encoding !== expected.encoding || row.total_bytes !== expected.length
              || row.digest !== expected.digest || row.state !== "deleting"))
            throw harnessError("INTERNAL_ERROR", "Retired report value identity changed");
        }
        await this.finishRetirement(input, epoch); completed++;
        cursor = row.operation_id as string;
      }
      if (rows.length < 64) return completed;
    }
  }

  private async finishRetirement(input: ReportTransferBinding, epoch: number): Promise<void> {
    await this.values.discard(input.control.id, input.control.owner);
    this.store.assertAuthority();
    if (this.store.coordinatorEpoch !== epoch) throw harnessError("EXECUTION_FENCED", "Report transfer collection authority changed");
    await this.values.discard(input.report.id, input.report.owner);
  }
}
