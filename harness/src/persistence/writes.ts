import type { DatabaseSync, SQLInputValue } from "node:sqlite";
import { canonicalJson, harnessError, type Json } from "../contract/index.js";

export interface SQLWrite {
  sql: string;
  params: (string | number | null | { json: Json } | { blob: string })[];
  /** Required for every write: zero rows must never silently pass a CAS. */
  expectChanges: number;
}

const tables = new Set(["commands", "runs", "frames", "actions", "facts", "frame_facts", "waits", "work_items", "frame_candidates", "prepare_rounds", "validation_jobs", "dispatch_preparations", "driver_jobs", "finalize_jobs", "start_jobs", "wake_jobs", "version_pointers", "definition_catalog", "node_sessions", "node_definitions", "submit_preparations", "business_leases", "business_attempts", "action_attempt_state", "action_reports", "report_processing", "action_observations", "action_measurements", "action_retry_authorizations", "action_recheck_requests", "capacity_reconciliation_support", "remote_run_links", "cancellation_barriers", "response_limit_watermark", "node_claim_gates", "value_capacity", "value_manifests", "value_chunks", "report_transfer_bindings", "report_transfer_diagnostics", "report_direct_diagnostics", "value_projections", "value_projection_bindings", "action_controls"]);
tables.add("frame_result_projections");
tables.add("driver_value_staging");
tables.add("action_request_staging");
tables.add("submit_input_staging");
tables.add("root_run_inputs");
tables.add("command_value_staging");
tables.add("checkpoint_value_staging");
tables.add("validation_value_staging");
tables.add("node_policy_staging");
const scalar = /^(?:\?|NULL|TRUE|FALSE|-?\d+(?:\.\d+)?|'(?:[^']|'')*'|[a-z_]+\s*[+-]\s*\d+)$/i;

/** Private, deliberately small DML grammar; JSON cells can only receive typed bindings or SQL NULL. */
export function compileWrite(db: DatabaseSync, write: SQLWrite) {
  const invalid = (): never => { throw harnessError("INVALID_ARGUMENT", "Invalid persistence write or untyped JSON binding"); };
  if (!write || typeof write.sql !== "string" || /;|--|\/\*|\bRETURNING\b/i.test(write.sql)
      || !Array.isArray(write.params) || !Number.isSafeInteger(write.expectChanges) || write.expectChanges < 0) invalid();
  const sql = write.sql.trim();
  const table = /^(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([a-z_]+)\b/i.exec(sql)?.[1]?.toLowerCase();
  if (!table || !tables.has(table)) invalid();
  const schema = db.prepare(`PRAGMA table_info(${table})`).all();
  const columns = schema.map(row => row.name as string);
  const integers = new Set(schema.filter(row => row.type === "INTEGER").map(row => row.name));
  const targets: { column: string; expression: string }[] = [];
  let where = "";
  const insert = /^INSERT\s+INTO\s+[a-z_]+\s*(?:\(([^)]+)\))?\s+VALUES\s*\((.*)\)$/is.exec(sql);
  const update = /^UPDATE\s+[a-z_]+\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$/is.exec(sql);
  const deletion = /^DELETE\s+FROM\s+[a-z_]+(?:\s+WHERE\s+(.+))?$/is.exec(sql);
  if (insert) {
    const names = insert[1] ? insert[1].split(",").map(value => value.trim().toLowerCase()) : columns;
    const expressions = splitExpressions(insert[2]!);
    if (names.length !== expressions.length || new Set(names).size !== names.length) invalid();
    names.forEach((column, i) => targets.push({ column, expression: expressions[i]!.trim() }));
  } else if (update) {
    for (const assignment of splitExpressions(update[1]!)) {
      const match = /^\s*([a-z_]+)\s*=\s*(.+)\s*$/is.exec(assignment);
      if (!match) invalid();
      targets.push({ column: match![1]!.toLowerCase(), expression: match![2]!.trim() });
    }
    if (new Set(targets.map(target => target.column)).size !== targets.length) invalid();
    where = update[2] ?? "";
  } else if (deletion) {
    where = deletion[1] ?? "";
  } else invalid();

  // No subqueries/functions/numbered bindings in predicates. Extend this seam deliberately when a use case needs more.
  const maskedWhere = where.replace(/'(?:[^']|'')*'/g, "''");
  if (/[^a-z_\d\s?=<>!()+.*'/-]/i.test(maskedWhere) || /\?(?:\d)|\b(?:SELECT|FROM|RETURNING|SET|VALUES)\b|[a-z_]\w*\s*\(/i.test(maskedWhere)) invalid();
  const params: SQLInputValue[] = [];
  const rangeGuards: string[] = [];
  let offset = 0;
  for (const { column, expression } of targets) {
    if (!columns.includes(column) || !scalar.test(expression)) invalid();
    if (column.endsWith("_json") && expression !== "?" && !/^NULL$/i.test(expression)) invalid();
    if (/^-?\d/.test(expression)) {
      const numeric = Number(expression);
      if (!Number.isFinite(numeric) || (Number.isInteger(numeric) && !Number.isSafeInteger(numeric))) invalid();
    }
    const arithmetic = /^([a-z_]+)\s*([+-])\s*(\d+)$/i.exec(expression);
    if (arithmetic) {
      const amount = Number(arithmetic[3]);
      if (!update || !integers.has(column) || arithmetic[1]!.toLowerCase() !== column || !Number.isSafeInteger(amount)) invalid();
      const delta = arithmetic[2] === "+" ? amount : -amount;
      const minimum = delta < 0 ? Number.MIN_SAFE_INTEGER - delta : Number.MIN_SAFE_INTEGER;
      const maximum = delta > 0 ? Number.MAX_SAFE_INTEGER - delta : Number.MAX_SAFE_INTEGER;
      // SQLite evaluates this predicate before assignment. An exhausted integer range is a failed CAS.
      rangeGuards.push(`${column} BETWEEN ${minimum} AND ${maximum}`);
    }
    if (expression === "?") {
      const value = write.params[offset++];
      if (column.endsWith("_blob")) {
        if (!isBlobBinding(value)) invalid();
        const encoded = (value as { blob: string }).blob;
        const decoded = Buffer.from(encoded, "base64");
        if (!decoded.length || decoded.length > 65536 || decoded.toString("base64") !== encoded) invalid();
        params.push(decoded);
      } else if (column.endsWith("_json")) {
        if (value === null) params.push(null);
        else if (isJsonBinding(value)) params.push(canonicalJson(value.json));
        else invalid();
      } else {
        if (value === null || typeof value === "string" || typeof value === "number") params.push(value);
        else invalid();
      }
    }
  }
  const whereBindings = (maskedWhere.match(/\?/g) ?? []).length;
  for (let i = 0; i < whereBindings; i++) {
    const value = write.params[offset++];
    if (value === null || typeof value === "string" || typeof value === "number") params.push(value);
    else invalid();
  }
  if (offset !== write.params.length) invalid();
  const guardedSql = rangeGuards.length ? `UPDATE ${table} SET ${update![1]} WHERE (${where || "1"}) AND ${rangeGuards.join(" AND ")}` : sql;
  try { return { statement: db.prepare(guardedSql), params, changes: write.expectChanges }; }
  catch { throw harnessError("INVALID_ARGUMENT", "Invalid persistence SQL statement"); }
}

function isJsonBinding(value: unknown): value is { json: Json } {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    && Object.keys(value).length === 1 && Object.hasOwn(value, "json");
}
function isBlobBinding(value: unknown): value is { blob: string } {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    && Object.keys(value).length === 1 && Object.hasOwn(value, "blob") && typeof (value as { blob?: unknown }).blob === "string";
}
function splitExpressions(value: string): string[] {
  // Commas inside SQL string literals are data; parentheses in expressions are rejected by the grammar.
  return value.match(/(?:'(?:[^']|'')*'|[^',])+/g) ?? [];
}
