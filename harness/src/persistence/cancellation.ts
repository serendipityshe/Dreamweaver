import type { DatabaseSync } from "node:sqlite";
import type { SQLWrite } from "./writes.js";
import { harnessError, parseJson, type CancellationInfo } from "../contract/index.js";

export type CancellationSubject = { kind: "run" | "action"; id: string };
export function cancellationGuard(subject: CancellationSubject): SQLWrite {
  checkSubject(subject);
  return { sql: "CHECK_EFFECTIVE_CANCELLATION", params: [subject.kind, subject.id], expectChanges: 1 };
}
const prefix = `WITH RECURSIVE
args(kind,subject_id) AS (SELECT ?,?),
target(kind,subject_id,run_id) AS (
  SELECT kind,subject_id,CASE WHEN kind='run' THEN subject_id ELSE (SELECT run_id FROM actions WHERE id=subject_id) END FROM args
),
lineage(run_id,parent_action_id) AS (
  SELECT r.id,r.parent_action_id FROM runs r,target t WHERE r.id=t.run_id
  UNION
  SELECT p.id,p.parent_action_id FROM lineage l JOIN actions a ON a.id=l.parent_action_id JOIN runs p ON p.id=a.run_id
),
ancestors(run_id,ancestor_id,parent_action_id) AS (
  SELECT run_id,run_id,parent_action_id FROM lineage
  UNION
  SELECT x.run_id,p.id,p.parent_action_id FROM ancestors x JOIN actions a ON a.id=x.parent_action_id JOIN runs p ON p.id=a.run_id
)`;
const invalid = `(
  NOT EXISTS (SELECT 1 FROM lineage)
  OR NOT EXISTS (SELECT 1 FROM lineage WHERE parent_action_id IS NULL)
  OR EXISTS (SELECT 1 FROM lineage l LEFT JOIN actions a ON a.id=l.parent_action_id WHERE l.parent_action_id IS NOT NULL AND a.id IS NULL)
  OR EXISTS (SELECT 1 FROM lineage l JOIN runs r ON r.id=l.run_id WHERE r.business_cancelled=1
    AND NOT EXISTS (SELECT 1 FROM ancestors x JOIN cancellation_barriers b ON
      (b.scope='run' AND b.run_id=x.ancestor_id)
      OR (b.scope='remaining_actions' AND b.run_id=x.ancestor_id AND x.ancestor_id<>r.id)
      OR (b.scope='action' AND b.action_id=x.parent_action_id)
      WHERE x.run_id=r.id)
    AND NOT (r.business_terminal=1 AND EXISTS (SELECT 1 FROM cancellation_barriers b WHERE b.scope='remaining_actions' AND b.run_id=r.id)))
  OR EXISTS (SELECT 1 FROM target t JOIN actions a ON t.kind='action' AND a.id=t.subject_id WHERE a.business_cancelled=1
    AND NOT EXISTS (SELECT 1 FROM cancellation_barriers b WHERE b.scope='action' AND b.action_id=a.id)
    AND NOT EXISTS (SELECT 1 FROM cancellation_barriers b JOIN lineage l ON l.run_id=b.run_id WHERE b.scope IN ('run','remaining_actions')))
)`;
const selectedWhere = `
  (b.scope='run' AND b.run_id IN (SELECT run_id FROM lineage))
  OR (b.scope='remaining_actions' AND b.run_id IN (SELECT run_id FROM lineage) AND (t.kind='action' OR b.run_id<>t.run_id))
  OR (b.scope='action' AND ((t.kind='action' AND b.action_id=t.subject_id) OR b.action_id IN (SELECT parent_action_id FROM lineage WHERE parent_action_id IS NOT NULL)))`;
const selected = (column: string) => `(SELECT b.${column} FROM cancellation_barriers b,target t WHERE ${selectedWhere} ORDER BY b.sequence LIMIT 1)`;
const projectionColumns = `${invalid} invalid, ${selected("cancellation_json")} cancellation_json, ${selected("scope")} barrier_scope, ${selected("run_id")} barrier_run_id, ${selected("action_id")} barrier_action_id`;
const resolveSQL = `${prefix} SELECT ${projectionColumns}, (SELECT run_id FROM target) target_run_id`;
const projectionSQL = `${prefix} SELECT ${projectionColumns}, t.run_id target_run_id,
  CASE WHEN t.kind='run' THEN (SELECT snapshot_json FROM runs WHERE id=t.subject_id) ELSE (SELECT snapshot_json FROM actions WHERE id=t.subject_id) END snapshot_json FROM target t`;

/** One shared resolver is used by locked guards and read projections. */
export class CancellationResolver {
  private readonly resolve;
  private readonly projection;
  constructor(db: DatabaseSync) { this.resolve = db.prepare(resolveSQL); this.projection = db.prepare(projectionSQL); }
  lookup(subject: CancellationSubject): { invalid: boolean; cancellationJSON: string | null } {
    checkSubject(subject);
    const row = this.resolve.get(subject.kind, subject.id)!;
    return { invalid: row.invalid === 1, cancellationJSON: row.cancellation_json as string | null };
  }
  read(subject: CancellationSubject): { snapshotJson: string; cancellation: CancellationInfo | null } | null {
    checkSubject(subject);
    const row = this.projection.get(subject.kind, subject.id)!;
    if (typeof row.snapshot_json !== "string") return null;
    if (row.invalid === 1) throw harnessError("EXECUTION_FENCED", "Cancellation ancestry is unavailable or invalid");
    return { snapshotJson: row.snapshot_json, cancellation: project(subject, row) };
  }
  value(subject: CancellationSubject): CancellationInfo | null {
    checkSubject(subject);
    const row = this.resolve.get(subject.kind, subject.id)!;
    if (row.invalid === 1) throw harnessError("EXECUTION_FENCED", "Cancellation ancestry is unavailable or invalid");
    return project(subject, row);
  }
}
function project(subject: CancellationSubject, row: Record<string, unknown>): CancellationInfo | null {
  if (row.cancellation_json === null) return null;
  const cancellation = parseJson(row.cancellation_json as string) as unknown as CancellationInfo;
  if (!cancellation || !["client", "parent", "completion", "agent"].includes(cancellation.source) || cancellation.reason !== null && typeof cancellation.reason !== "string" || !Number.isSafeInteger(cancellation.requestedAt) || cancellation.requestedAt < 0) throw harnessError("INTERNAL_ERROR", "Invalid cancellation barrier");
  const direct = row.barrier_scope === "run" && subject.kind === "run" && row.barrier_run_id === row.target_run_id
    || row.barrier_scope === "action" && subject.kind === "action" && row.barrier_action_id === subject.id;
  if (direct) return cancellation;
  return { ...cancellation, source: row.barrier_scope === "remaining_actions" && subject.kind === "action" && row.barrier_run_id === row.target_run_id ? "completion" : "parent" };
}
function checkSubject(subject: CancellationSubject): void {
  if (!subject || !["run", "action"].includes(subject.kind) || typeof subject.id !== "string" || !subject.id || subject.id.trim() !== subject.id) throw harnessError("INVALID_ARGUMENT", "Invalid cancellation subject");
}
