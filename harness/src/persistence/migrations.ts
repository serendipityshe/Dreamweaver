import { createHash, randomBytes, randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
import { canonicalJson, parseJson, type Json } from "../contract/index.js";
import { continuationMigrationSQL } from "./continuation-migration.js";
import { checkpointValueMigrationSQL } from "./checkpoint-value-migration.js";
import { commandValueMigration } from "./command-value.js";
import { policyValueMigration } from "./policy-value-migration.js";
import { validationValueMigrationSQL } from "./validation-value.js";

// Internal schema only. New tasks extend it through new, immutable migrations.
const migrations = [
  { version: 1, name: "atomic-foundation", sql: `
CREATE TABLE domain (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  domain_id TEXT NOT NULL UNIQUE CHECK (length(domain_id) > 0),
  protocol_version TEXT NOT NULL CHECK (protocol_version = 'harness/1'),
  cursor_secret TEXT NOT NULL CHECK (length(cursor_secret) = 64)
) STRICT;
CREATE TABLE commands (
  principal_id TEXT NOT NULL,
  request_key TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('submit','cancel','resumeRun','recheckAction','resolveAction','adoptVersion')),
  request_json TEXT NOT NULL CHECK (json_valid(request_json)),
  fingerprint TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending','applied','rejected')),
  result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
  error_json TEXT CHECK (error_json IS NULL OR json_valid(error_json)),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (principal_id, request_key),
  CHECK ((status = 'pending' AND result_json IS NULL AND error_json IS NULL)
      OR (status = 'applied' AND result_json IS NOT NULL AND error_json IS NULL)
      OR (status = 'rejected' AND result_json IS NULL AND error_json IS NOT NULL
          AND json_extract(error_json, '$.disposition') IS 'rejected'))
) STRICT;
CREATE TABLE runs (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  epoch INTEGER NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  frame_count INTEGER NOT NULL DEFAULT 0 CHECK (frame_count >= 0),
  snapshot_json TEXT NOT NULL CHECK (json_valid(snapshot_json))
) STRICT;
CREATE TABLE frames (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  number INTEGER NOT NULL CHECK (number > 0),
  input_json TEXT NOT NULL CHECK (json_valid(input_json)),
  state TEXT NOT NULL CHECK (state IN ('open','committed')),
  UNIQUE (run_id, number), UNIQUE (id, run_id)
) STRICT;
CREATE TRIGGER frames_input_immutable BEFORE UPDATE OF id, run_id, number, input_json ON frames
BEGIN SELECT RAISE(ABORT, 'immutable frame input'); END;
CREATE TABLE actions (
  creation_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL UNIQUE,
  run_id TEXT NOT NULL REFERENCES runs(id),
  action_key TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  epoch INTEGER NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  request_json TEXT NOT NULL CHECK (json_valid(request_json)),
  outcome_json TEXT CHECK (outcome_json IS NULL OR json_valid(outcome_json)),
  UNIQUE (run_id, action_key), UNIQUE (id, run_id)
) STRICT;
CREATE TABLE facts (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  action_id TEXT NOT NULL UNIQUE,
  outcome_json TEXT NOT NULL CHECK (json_valid(outcome_json)),
  FOREIGN KEY (action_id, run_id) REFERENCES actions(id, run_id),
  UNIQUE (id, run_id)
) STRICT;
CREATE TRIGGER facts_immutable BEFORE UPDATE ON facts
BEGIN SELECT RAISE(ABORT, 'immutable fact'); END;
CREATE TABLE frame_facts (
  frame_id TEXT NOT NULL,
  fact_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  PRIMARY KEY (frame_id, fact_id),
  FOREIGN KEY (frame_id, run_id) REFERENCES frames(id, run_id),
  FOREIGN KEY (fact_id, run_id) REFERENCES facts(id, run_id)
) STRICT;
CREATE TABLE work_items (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  available_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json))
) STRICT;
CREATE INDEX work_items_ready ON work_items(stage, available_at, id);
CREATE TABLE history (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL UNIQUE,
  operation_id TEXT UNIQUE,
  type TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  at INTEGER NOT NULL,
  request_json TEXT NOT NULL CHECK (json_valid(request_json)),
  data_json TEXT NOT NULL CHECK (json_valid(data_json)),
  result_json TEXT NOT NULL CHECK (json_valid(result_json))
) STRICT;
CREATE INDEX history_subject ON history(subject_id, sequence);
CREATE TRIGGER history_no_update BEFORE UPDATE ON history
BEGIN SELECT RAISE(ABORT, 'append-only history'); END;
CREATE TRIGGER history_no_delete BEFORE DELETE ON history
BEGIN SELECT RAISE(ABORT, 'append-only history'); END;
CREATE INDEX commands_pending ON commands(status, updated_at);
CREATE INDEX runs_principal ON runs(principal_id, id);
CREATE INDEX frames_open ON frames(run_id, state, number);
CREATE TRIGGER command_identity_immutable BEFORE UPDATE OF principal_id, request_key, operation, request_json, fingerprint, created_at ON commands
BEGIN SELECT RAISE(ABORT, 'immutable command identity'); END;
CREATE TRIGGER commands_retained BEFORE DELETE ON commands
BEGIN SELECT RAISE(ABORT, 'retain command until domain reset'); END;
CREATE TRIGGER frames_retained BEFORE DELETE ON frames
BEGIN SELECT RAISE(ABORT, 'retain frame until domain reset'); END;
CREATE TRIGGER facts_retained BEFORE DELETE ON facts
BEGIN SELECT RAISE(ABORT, 'retain fact until domain reset'); END;
` },
  { version: 2, name: "bounded-preparation", sql: `
CREATE TABLE frame_candidates (
  frame_id TEXT PRIMARY KEY REFERENCES frames(id),
  decision_json TEXT NOT NULL CHECK (json_valid(decision_json)),
  created_at INTEGER NOT NULL
) STRICT;
CREATE TRIGGER candidates_immutable BEFORE UPDATE ON frame_candidates
BEGIN SELECT RAISE(ABORT, 'immutable candidate'); END;
CREATE TRIGGER candidates_retained BEFORE DELETE ON frame_candidates
BEGIN SELECT RAISE(ABORT, 'retain candidate until domain reset'); END;
CREATE TABLE prepare_rounds (
  id TEXT PRIMARY KEY,
  owner_kind TEXT NOT NULL CHECK (owner_kind IN ('command','frame','report')),
  owner_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  phase TEXT NOT NULL,
  number INTEGER NOT NULL CHECK (number > 0),
  prepare_timeout_ms INTEGER NOT NULL CHECK (prepare_timeout_ms > 0),
  created_at INTEGER NOT NULL,
  deadline_at INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','accepted','expired','superseded')),
  CHECK (deadline_at = created_at + prepare_timeout_ms),
  UNIQUE (owner_kind, owner_id, number)
) STRICT;
CREATE UNIQUE INDEX prepare_rounds_active ON prepare_rounds(owner_kind, owner_id) WHERE state = 'pending';
CREATE INDEX prepare_rounds_deadline ON prepare_rounds(state, deadline_at);
CREATE TRIGGER round_identity_immutable BEFORE UPDATE OF id, owner_kind, owner_id, principal_id, phase, number, prepare_timeout_ms, created_at, deadline_at ON prepare_rounds
BEGIN SELECT RAISE(ABORT, 'immutable preparation deadline'); END;
CREATE TABLE validation_jobs (
  id TEXT PRIMARY KEY,
  round_id TEXT NOT NULL REFERENCES prepare_rounds(id),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  state TEXT NOT NULL CHECK (state IN ('pending','assigned','accepted','superseded')),
  assignment_id TEXT UNIQUE,
  assignment_epoch INTEGER NOT NULL DEFAULT 0 CHECK (assignment_epoch >= 0),
  node_incarnation TEXT,
  assignment_deadline_at INTEGER,
  report_json TEXT CHECK (report_json IS NULL OR json_valid(report_json)),
  CHECK ((state = 'pending' AND assignment_id IS NULL AND assignment_epoch = 0 AND node_incarnation IS NULL AND assignment_deadline_at IS NULL AND report_json IS NULL)
      OR (state IN ('assigned','accepted','superseded') AND assignment_id IS NOT NULL AND assignment_epoch > 0 AND node_incarnation IS NOT NULL AND assignment_deadline_at IS NOT NULL)),
  CHECK ((state = 'accepted' AND report_json IS NOT NULL)
      OR (state IN ('pending','assigned') AND report_json IS NULL)
      OR state = 'superseded')
) STRICT;
CREATE INDEX validation_jobs_round ON validation_jobs(round_id, state);
CREATE INDEX validation_jobs_deadline ON validation_jobs(state, assignment_deadline_at);
CREATE TRIGGER job_payload_immutable BEFORE UPDATE OF id, round_id, payload_json ON validation_jobs
BEGIN SELECT RAISE(ABORT, 'immutable validation payload'); END;
CREATE TRIGGER assignment_within_round_insert BEFORE INSERT ON validation_jobs
WHEN NEW.assignment_deadline_at > (SELECT deadline_at FROM prepare_rounds WHERE id = NEW.round_id)
BEGIN SELECT RAISE(ABORT, 'assignment exceeds round deadline'); END;
CREATE TRIGGER assignment_within_round_update BEFORE UPDATE ON validation_jobs
WHEN NEW.assignment_deadline_at > (SELECT deadline_at FROM prepare_rounds WHERE id = NEW.round_id)
BEGIN SELECT RAISE(ABORT, 'assignment exceeds round deadline'); END;
` },
  { version: 3, name: "unassigned-job-supersession", sql: `
DROP INDEX validation_jobs_round;
DROP INDEX validation_jobs_deadline;
DROP TRIGGER job_payload_immutable;
DROP TRIGGER assignment_within_round_insert;
DROP TRIGGER assignment_within_round_update;
ALTER TABLE validation_jobs RENAME TO validation_jobs_v2;
CREATE TABLE validation_jobs (
  id TEXT PRIMARY KEY,
  round_id TEXT NOT NULL REFERENCES prepare_rounds(id),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  state TEXT NOT NULL CHECK (state IN ('pending','assigned','accepted','superseded')),
  assignment_id TEXT UNIQUE,
  assignment_epoch INTEGER NOT NULL DEFAULT 0 CHECK (assignment_epoch >= 0),
  node_incarnation TEXT,
  assignment_deadline_at INTEGER,
  report_json TEXT CHECK (report_json IS NULL OR json_valid(report_json)),
  CHECK ((state IN ('pending','superseded') AND assignment_id IS NULL AND assignment_epoch = 0 AND node_incarnation IS NULL AND assignment_deadline_at IS NULL AND report_json IS NULL)
      OR (state IN ('assigned','accepted','superseded') AND assignment_id IS NOT NULL AND assignment_epoch > 0 AND node_incarnation IS NOT NULL AND assignment_deadline_at IS NOT NULL)),
  CHECK ((state = 'accepted' AND report_json IS NOT NULL)
      OR (state IN ('pending','assigned') AND report_json IS NULL)
      OR state = 'superseded')
) STRICT;
INSERT INTO validation_jobs SELECT * FROM validation_jobs_v2;
DROP TABLE validation_jobs_v2;
CREATE INDEX validation_jobs_round ON validation_jobs(round_id, state);
CREATE INDEX validation_jobs_deadline ON validation_jobs(state, assignment_deadline_at);
CREATE TRIGGER job_payload_immutable BEFORE UPDATE OF id, round_id, payload_json ON validation_jobs
BEGIN SELECT RAISE(ABORT, 'immutable validation payload'); END;
CREATE TRIGGER assignment_within_round_insert BEFORE INSERT ON validation_jobs
WHEN NEW.assignment_deadline_at > (SELECT deadline_at FROM prepare_rounds WHERE id = NEW.round_id)
BEGIN SELECT RAISE(ABORT, 'assignment exceeds round deadline'); END;
CREATE TRIGGER assignment_within_round_update BEFORE UPDATE ON validation_jobs
WHEN NEW.assignment_deadline_at > (SELECT deadline_at FROM prepare_rounds WHERE id = NEW.round_id)
BEGIN SELECT RAISE(ABORT, 'assignment exceeds round deadline'); END;
` },
  { version: 4, name: "coordinator-fence-and-semantic-receipts", sql: `
ALTER TABLE domain ADD COLUMN coordinator_epoch INTEGER NOT NULL DEFAULT 0 CHECK (coordinator_epoch >= 0);
ALTER TABLE history ADD COLUMN request_hash TEXT NOT NULL DEFAULT '';
DROP TRIGGER history_no_update;
-- migrate() converts legacy batch envelopes to canonical semantic requests (v1).
` },
  { version: 5, name: "queryable-action-views-and-version-pointers", sql: `
ALTER TABLE actions ADD COLUMN snapshot_json TEXT CHECK (snapshot_json IS NULL OR json_valid(snapshot_json));
CREATE TABLE version_pointers (
  kind TEXT NOT NULL CHECK (kind IN ('agent','capacity')),
  id TEXT NOT NULL CHECK (length(id) > 0),
  version TEXT NOT NULL CHECK (length(version) > 0),
  revision INTEGER NOT NULL CHECK (revision > 0),
  evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
  updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
  PRIMARY KEY (kind, id)
) STRICT;
` },
  { version: 6, name: "durable-node-definition-directory", sql: `
CREATE TABLE definition_catalog (
  kind TEXT NOT NULL CHECK (kind IN ('agent','capacity','policy')),
  id TEXT NOT NULL,
  version TEXT NOT NULL,
  registered_at INTEGER NOT NULL CHECK (registered_at >= 0),
  PRIMARY KEY (kind,id,version)
) STRICT;
CREATE TRIGGER definitions_immutable BEFORE UPDATE ON definition_catalog
BEGIN SELECT RAISE(ABORT, 'immutable definition'); END;
CREATE TRIGGER definitions_retained BEFORE DELETE ON definition_catalog
BEGIN SELECT RAISE(ABORT, 'retain definition'); END;
CREATE TABLE node_sessions (
  incarnation TEXT PRIMARY KEY,
  node_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('online','offline')),
  expires_at INTEGER NOT NULL CHECK (expires_at >= 0),
  registered_at INTEGER NOT NULL CHECK (registered_at >= 0),
  configuration_json TEXT NOT NULL CHECK (json_valid(configuration_json)),
  UNIQUE (node_id,incarnation)
) STRICT;
CREATE INDEX node_sessions_active ON node_sessions(node_id,state,expires_at);
CREATE UNIQUE INDEX node_sessions_online ON node_sessions(node_id) WHERE state='online';
CREATE TRIGGER node_identity_immutable BEFORE UPDATE OF incarnation,node_id,principal_id,registered_at,configuration_json ON node_sessions
BEGIN SELECT RAISE(ABORT, 'immutable node identity'); END;
CREATE TABLE node_definitions (
  incarnation TEXT NOT NULL REFERENCES node_sessions(incarnation),
  kind TEXT NOT NULL,
  id TEXT NOT NULL,
  version TEXT NOT NULL,
  PRIMARY KEY (incarnation,kind,id,version),
  FOREIGN KEY (kind,id,version) REFERENCES definition_catalog(kind,id,version)
) STRICT;
CREATE INDEX node_definition_routes ON node_definitions(kind,id,version,incarnation);
CREATE TRIGGER node_definitions_immutable BEFORE UPDATE ON node_definitions
BEGIN SELECT RAISE(ABORT, 'immutable node definitions'); END;
CREATE TRIGGER node_definitions_retained BEFORE DELETE ON node_definitions
BEGIN SELECT RAISE(ABORT, 'retain node definitions'); END;
` },
  { version: 7, name: "durable-submit-preparation", sql: `
CREATE TABLE submit_preparations (
  principal_id TEXT NOT NULL,
  request_key TEXT NOT NULL,
  round_id TEXT NOT NULL UNIQUE REFERENCES prepare_rounds(id),
  candidate_json TEXT CHECK (candidate_json IS NULL OR json_valid(candidate_json)),
  pointer_revision INTEGER CHECK (pointer_revision IS NULL OR pointer_revision > 0),
  limits_json TEXT NOT NULL CHECK (json_valid(limits_json)),
  PRIMARY KEY (principal_id, request_key),
  FOREIGN KEY (principal_id, request_key) REFERENCES commands(principal_id, request_key)
) STRICT;
CREATE TRIGGER submit_preparation_identity_immutable BEFORE UPDATE OF principal_id, request_key, round_id, limits_json ON submit_preparations
BEGIN SELECT RAISE(ABORT, 'immutable submit preparation identity'); END;
CREATE TRIGGER submit_preparations_retained BEFORE DELETE ON submit_preparations
BEGIN SELECT RAISE(ABORT, 'retain submit preparation'); END;
` },
  { version: 8, name: "bounded-submit-queue", sql: `
CREATE INDEX commands_submit_queue ON commands(operation,status,created_at,principal_id,request_key);
` },
  { version: 9, name: "driver-frame-work", sql: `
ALTER TABLE runs ADD COLUMN core_json TEXT CHECK (core_json IS NULL OR json_valid(core_json));
CREATE TABLE driver_jobs (
  id TEXT PRIMARY KEY REFERENCES frames(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  agent_id TEXT NOT NULL,
  agent_version TEXT NOT NULL,
  coordinator_epoch INTEGER NOT NULL,
  principal_id TEXT NOT NULL,
  input_json TEXT NOT NULL CHECK (json_valid(input_json)),
  state TEXT NOT NULL CHECK (state IN ('pending','assigned','accepted','superseded')),
  assignment_id TEXT,
  assignment_epoch INTEGER NOT NULL DEFAULT 0,
  node_incarnation TEXT REFERENCES node_sessions(incarnation),
  assignment_deadline_at INTEGER,
  report_json TEXT CHECK (report_json IS NULL OR json_valid(report_json)),
  CHECK ((state='pending' AND assignment_id IS NULL AND assignment_epoch=0 AND node_incarnation IS NULL AND assignment_deadline_at IS NULL AND report_json IS NULL)
    OR (state='assigned' AND assignment_id IS NOT NULL AND assignment_epoch>0 AND node_incarnation IS NOT NULL AND assignment_deadline_at IS NOT NULL AND report_json IS NULL)
    OR (state='accepted' AND assignment_id IS NOT NULL AND assignment_epoch>0 AND node_incarnation IS NOT NULL AND assignment_deadline_at IS NOT NULL AND report_json IS NOT NULL)
    OR (state='superseded' AND report_json IS NULL))
) STRICT;
CREATE INDEX work_items_subject_stage ON work_items(kind,subject_id,stage);
CREATE INDEX driver_jobs_ready ON driver_jobs(coordinator_epoch,state,agent_id,agent_version,id);
CREATE INDEX driver_jobs_expired ON driver_jobs(coordinator_epoch,state,agent_id,agent_version,assignment_deadline_at,id);
CREATE TRIGGER driver_job_identity_immutable BEFORE UPDATE OF id,run_id,agent_id,agent_version,coordinator_epoch,principal_id,input_json ON driver_jobs
BEGIN SELECT RAISE(ABORT, 'immutable driver job identity'); END;
CREATE TRIGGER driver_jobs_retained BEFORE DELETE ON driver_jobs
BEGIN SELECT RAISE(ABORT, 'retain driver job'); END;
` },
  { version: 10, name: "bounded-run-queue", sql: `
CREATE INDEX work_items_run_queue ON work_items(available_at,id) WHERE kind='run' AND stage IN ('start','driver','validation','complete');
` },
  { version: 11, name: "business-execution-leases", sql: `
ALTER TABLE driver_jobs ADD COLUMN lease_epoch INTEGER;
ALTER TABLE driver_jobs ADD COLUMN assignment_json TEXT CHECK (assignment_json IS NULL OR json_valid(assignment_json));
ALTER TABLE driver_jobs ADD COLUMN grant_json TEXT CHECK (grant_json IS NULL OR json_valid(grant_json));
ALTER TABLE runs ADD COLUMN business_cancelled INTEGER NOT NULL DEFAULT 0 CHECK (business_cancelled IN (0,1));
ALTER TABLE runs ADD COLUMN business_terminal INTEGER NOT NULL DEFAULT 0 CHECK (business_terminal IN (0,1));
ALTER TABLE actions ADD COLUMN business_cancelled INTEGER NOT NULL DEFAULT 0 CHECK (business_cancelled IN (0,1));
ALTER TABLE actions ADD COLUMN business_terminal INTEGER NOT NULL DEFAULT 0 CHECK (business_terminal IN (0,1));
UPDATE runs SET business_cancelled=CASE WHEN json_extract(snapshot_json,'$.cancelRequested') IS 1 THEN 1 ELSE 0 END,
  business_terminal=CASE WHEN json_extract(snapshot_json,'$.outcome') IS NOT NULL THEN 1 ELSE 0 END;
UPDATE actions SET business_cancelled=CASE WHEN json_extract(snapshot_json,'$.cancelRequested') IS 1 THEN 1 ELSE 0 END,
  business_terminal=CASE WHEN outcome_json IS NOT NULL OR json_extract(snapshot_json,'$.outcome') IS NOT NULL THEN 1 ELSE 0 END;
CREATE TRIGGER run_business_state_insert AFTER INSERT ON runs BEGIN
  UPDATE runs SET business_cancelled=CASE WHEN json_extract(NEW.snapshot_json,'$.cancelRequested') IS 1 THEN 1 ELSE 0 END,
    business_terminal=CASE WHEN json_extract(NEW.snapshot_json,'$.outcome') IS NOT NULL THEN 1 ELSE 0 END WHERE id=NEW.id;
END;
CREATE TRIGGER run_business_state_update AFTER UPDATE OF snapshot_json ON runs BEGIN
  UPDATE runs SET business_cancelled=CASE WHEN json_extract(NEW.snapshot_json,'$.cancelRequested') IS 1 THEN 1 ELSE 0 END,
    business_terminal=CASE WHEN json_extract(NEW.snapshot_json,'$.outcome') IS NOT NULL THEN 1 ELSE 0 END WHERE id=NEW.id;
END;
CREATE TRIGGER action_business_state_insert AFTER INSERT ON actions BEGIN
  UPDATE actions SET business_cancelled=CASE WHEN json_extract(NEW.snapshot_json,'$.cancelRequested') IS 1 THEN 1 ELSE 0 END,
    business_terminal=CASE WHEN NEW.outcome_json IS NOT NULL OR json_extract(NEW.snapshot_json,'$.outcome') IS NOT NULL THEN 1 ELSE 0 END WHERE id=NEW.id;
END;
CREATE TRIGGER action_business_state_update AFTER UPDATE OF snapshot_json,outcome_json ON actions BEGIN
  UPDATE actions SET business_cancelled=CASE WHEN json_extract(NEW.snapshot_json,'$.cancelRequested') IS 1 THEN 1 ELSE 0 END,
    business_terminal=CASE WHEN NEW.outcome_json IS NOT NULL OR json_extract(NEW.snapshot_json,'$.outcome') IS NOT NULL THEN 1 ELSE 0 END WHERE id=NEW.id;
END;
CREATE TABLE business_leases (
  kind TEXT NOT NULL CHECK (kind IN ('run','action')),
  subject_id TEXT NOT NULL,
  epoch INTEGER NOT NULL CHECK (epoch > 0),
  node_id TEXT NOT NULL,
  worker_principal_id TEXT NOT NULL,
  incarnation TEXT NOT NULL REFERENCES node_sessions(incarnation),
  expires_at INTEGER NOT NULL,
  released INTEGER NOT NULL CHECK (released IN (0,1)),
  attempt_id TEXT,
  attempt_number INTEGER,
  PRIMARY KEY (kind,subject_id),
  CHECK ((kind='run' AND attempt_id IS NULL AND attempt_number IS NULL) OR
         (kind='action' AND attempt_id IS NOT NULL AND attempt_number > 0))
) STRICT;
CREATE INDEX business_leases_expiry ON business_leases(kind,released,expires_at,subject_id);
CREATE TABLE business_attempts (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL REFERENCES actions(id),
  number INTEGER NOT NULL CHECK (number > 0),
  epoch INTEGER NOT NULL CHECK (epoch > 0),
  node_id TEXT NOT NULL,
  worker_principal_id TEXT NOT NULL,
  incarnation TEXT NOT NULL REFERENCES node_sessions(incarnation),
  started_at INTEGER NOT NULL,
  UNIQUE (action_id,number), UNIQUE (action_id,epoch)
) STRICT;
CREATE TRIGGER business_attempt_identity_immutable BEFORE UPDATE ON business_attempts
BEGIN SELECT RAISE(ABORT, 'immutable attempt identity'); END;
CREATE TRIGGER business_attempts_retained BEFORE DELETE ON business_attempts
BEGIN SELECT RAISE(ABORT, 'retain attempt'); END;
CREATE TABLE finalize_jobs (
  id TEXT PRIMARY KEY REFERENCES frames(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  agent_id TEXT NOT NULL,
  agent_version TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  input_json TEXT NOT NULL CHECK (json_valid(input_json)),
  coordinator_epoch INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','assigned','accepted')),
  assignment_id TEXT,
  assignment_epoch INTEGER NOT NULL DEFAULT 0,
  node_incarnation TEXT REFERENCES node_sessions(incarnation),
  assignment_deadline_at INTEGER,
  assignment_json TEXT CHECK (assignment_json IS NULL OR json_valid(assignment_json)),
  lease_epoch INTEGER,
  grant_json TEXT CHECK (grant_json IS NULL OR json_valid(grant_json))
) STRICT;
CREATE INDEX finalize_jobs_ready ON finalize_jobs(coordinator_epoch,state,agent_id,agent_version,id);
CREATE INDEX finalize_jobs_expired ON finalize_jobs(coordinator_epoch,state,agent_id,agent_version,assignment_deadline_at,id);
CREATE TRIGGER finalize_job_identity_immutable BEFORE UPDATE OF id,run_id,agent_id,agent_version,principal_id,input_json,coordinator_epoch ON finalize_jobs
BEGIN SELECT RAISE(ABORT, 'immutable finalize job identity'); END;
CREATE TRIGGER finalize_jobs_retained BEFORE DELETE ON finalize_jobs
BEGIN SELECT RAISE(ABORT, 'retain finalize job'); END;
CREATE TABLE start_jobs (
  id TEXT PRIMARY KEY REFERENCES runs(id),
  agent_id TEXT NOT NULL,
  agent_version TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  coordinator_epoch INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','assigned','accepted')),
  assignment_id TEXT,
  assignment_epoch INTEGER NOT NULL DEFAULT 0,
  node_incarnation TEXT REFERENCES node_sessions(incarnation),
  assignment_deadline_at INTEGER,
  assignment_json TEXT CHECK (assignment_json IS NULL OR json_valid(assignment_json)),
  grant_json TEXT CHECK (grant_json IS NULL OR json_valid(grant_json)),
  lease_epoch INTEGER
) STRICT;
CREATE INDEX start_jobs_ready ON start_jobs(coordinator_epoch,state,agent_id,agent_version,id);
CREATE TRIGGER start_job_identity_immutable BEFORE UPDATE OF id,agent_id,agent_version,principal_id,coordinator_epoch ON start_jobs
BEGIN SELECT RAISE(ABORT, 'immutable start job identity'); END;
CREATE TRIGGER start_jobs_retained BEFORE DELETE ON start_jobs
BEGIN SELECT RAISE(ABORT, 'retain start job'); END;
` },
  { version: 12, name: "dispatch-preparation", sql: `
CREATE TABLE dispatch_preparations (
  frame_id TEXT PRIMARY KEY REFERENCES frames(id),
  round_id TEXT NOT NULL UNIQUE REFERENCES prepare_rounds(id),
  policies_json TEXT NOT NULL CHECK (json_valid(policies_json)),
  original_json TEXT NOT NULL CHECK (json_valid(original_json)),
  rewritten_json TEXT CHECK (rewritten_json IS NULL OR json_valid(rewritten_json)),
  resolved_json TEXT CHECK (resolved_json IS NULL OR json_valid(resolved_json)),
  pointer_revisions_json TEXT CHECK (pointer_revisions_json IS NULL OR json_valid(pointer_revisions_json)),
  state TEXT NOT NULL CHECK (state IN ('rewrite','resolve','validation','ready')),
  generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0)
) STRICT;
CREATE TRIGGER dispatch_preparation_identity_immutable BEFORE UPDATE OF frame_id,round_id,policies_json,original_json ON dispatch_preparations
BEGIN SELECT RAISE(ABORT, 'immutable dispatch preparation'); END;
CREATE TRIGGER dispatch_preparations_retained BEFORE DELETE ON dispatch_preparations
BEGIN SELECT RAISE(ABORT, 'retain dispatch preparation'); END;
CREATE TABLE waits (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  frame_id TEXT NOT NULL REFERENCES frames(id),
  action_keys_json TEXT NOT NULL CHECK (json_valid(action_keys_json)),
  deadline_at INTEGER,
  winner TEXT CHECK (winner IN ('results','timeout')),
  wake_json TEXT CHECK (wake_json IS NULL OR json_valid(wake_json)),
  UNIQUE (frame_id)
) STRICT;
CREATE INDEX waits_deadline ON waits(winner,deadline_at,id);
CREATE INDEX work_items_run_dispatch_queue ON work_items(available_at,id) WHERE kind='run' AND stage='dispatch_prepare';
CREATE TRIGGER wait_identity_immutable BEFORE UPDATE OF id,run_id,frame_id,action_keys_json,deadline_at ON waits
BEGIN SELECT RAISE(ABORT, 'immutable wait identity'); END;
CREATE TRIGGER waits_retained BEFORE DELETE ON waits
BEGIN SELECT RAISE(ABORT, 'retain wait'); END;
` },
  { version: 13, name: "managed-capacity-attempts", sql: `
CREATE TABLE action_auth_targets (
  action_id TEXT PRIMARY KEY REFERENCES actions(id),
  capacity_id TEXT NOT NULL,
  capacity_version TEXT NOT NULL
) STRICT;
CREATE TABLE action_auth_policies (
  action_id TEXT NOT NULL REFERENCES actions(id),
  ordinal INTEGER NOT NULL,
  policy_id TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  PRIMARY KEY (action_id,ordinal)
) STRICT;
INSERT INTO action_auth_targets(action_id,capacity_id,capacity_version)
  SELECT id,json_extract(request_json,'$.target.id'),json_extract(request_json,'$.target.version') FROM actions
  WHERE json_type(request_json,'$.target.id')='text' AND json_type(request_json,'$.target.version')='text';
INSERT INTO action_auth_policies(action_id,ordinal,policy_id,policy_version)
  SELECT a.id,p.key,json_extract(a.request_json,'$.policies['||p.key||'].ref.id'),json_extract(a.request_json,'$.policies['||p.key||'].ref.version')
  FROM actions a,json_each(a.request_json,'$.policies') p WHERE typeof(p.key)='integer'
    AND json_type(a.request_json,'$.policies['||p.key||'].ref.id')='text' AND json_type(a.request_json,'$.policies['||p.key||'].ref.version')='text';
CREATE TRIGGER action_auth_insert AFTER INSERT ON actions BEGIN
  INSERT INTO action_auth_targets(action_id,capacity_id,capacity_version)
    SELECT NEW.id,json_extract(NEW.request_json,'$.target.id'),json_extract(NEW.request_json,'$.target.version')
    WHERE json_type(NEW.request_json,'$.target.id')='text' AND json_type(NEW.request_json,'$.target.version')='text';
  INSERT INTO action_auth_policies(action_id,ordinal,policy_id,policy_version)
    SELECT NEW.id,p.key,json_extract(NEW.request_json,'$.policies['||p.key||'].ref.id'),json_extract(NEW.request_json,'$.policies['||p.key||'].ref.version')
    FROM json_each(NEW.request_json,'$.policies') p WHERE typeof(p.key)='integer'
      AND json_type(NEW.request_json,'$.policies['||p.key||'].ref.id')='text' AND json_type(NEW.request_json,'$.policies['||p.key||'].ref.version')='text';
END;
CREATE TRIGGER action_request_immutable BEFORE UPDATE OF request_json ON actions
WHEN NEW.request_json<>OLD.request_json BEGIN SELECT RAISE(ABORT, 'immutable fixed action request'); END;
CREATE TABLE action_attempt_state (
  attempt_id TEXT PRIMARY KEY REFERENCES business_attempts(id),
  action_id TEXT NOT NULL REFERENCES actions(id),
  mode TEXT NOT NULL CHECK (mode IN ('automatic-recovery','recheck-only')),
  recovering INTEGER NOT NULL CHECK (recovering IN (0,1)),
  stage TEXT NOT NULL CHECK (stage IN ('execute','reconcile')),
  execute_dispatched INTEGER NOT NULL DEFAULT 0 CHECK (execute_dispatched IN (0,1)),
  dispatch_request_id TEXT,
  CHECK ((execute_dispatched=0 AND dispatch_request_id IS NULL) OR (execute_dispatched=1 AND dispatch_request_id IS NOT NULL))
) STRICT;
CREATE TRIGGER action_attempt_identity_immutable BEFORE UPDATE OF attempt_id,action_id,mode,recovering ON action_attempt_state
BEGIN SELECT RAISE(ABORT, 'immutable action attempt identity'); END;
CREATE TRIGGER action_attempts_retained BEFORE DELETE ON action_attempt_state
BEGIN SELECT RAISE(ABORT, 'retain action attempt'); END;
CREATE TABLE action_reports (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL REFERENCES actions(id),
  attempt_id TEXT NOT NULL REFERENCES business_attempts(id),
  phase TEXT NOT NULL CHECK (phase IN ('execute','reconcile')),
  assignment_id TEXT NOT NULL,
  report_json TEXT NOT NULL CHECK (json_valid(report_json)),
  authority_json TEXT CHECK (authority_json IS NULL OR json_valid(authority_json)),
  received_at INTEGER NOT NULL,
  UNIQUE (action_id,attempt_id,phase)
) STRICT;
CREATE TRIGGER action_reports_immutable BEFORE UPDATE ON action_reports
BEGIN SELECT RAISE(ABORT, 'immutable pending report'); END;
CREATE TRIGGER action_reports_retained BEFORE DELETE ON action_reports
BEGIN SELECT RAISE(ABORT, 'retain pending report'); END;
` },
  { version: 14, name: "structured-observation-processing", sql: `
CREATE TABLE report_processing (
  report_id TEXT PRIMARY KEY REFERENCES action_reports(id),
  state TEXT NOT NULL CHECK (state IN ('new','validation','deferred','done')),
  round_id TEXT REFERENCES prepare_rounds(id),
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0)
) STRICT;
CREATE INDEX report_processing_queue ON report_processing(state,report_id);
INSERT INTO report_processing(report_id,state) SELECT id,'new' FROM action_reports;
CREATE TRIGGER report_processing_insert AFTER INSERT ON action_reports BEGIN
  INSERT INTO report_processing(report_id,state) VALUES(NEW.id,'new');
END;
CREATE TABLE action_observations (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL UNIQUE,
  action_id TEXT NOT NULL REFERENCES actions(id),
  attempt_id TEXT REFERENCES business_attempts(id),
  source TEXT NOT NULL CHECK (source IN ('execute','reconcile','operator','remote')),
  source_id TEXT NOT NULL,
  snapshot_json TEXT NOT NULL CHECK (json_valid(snapshot_json)),
  UNIQUE (source,source_id)
) STRICT;
CREATE TRIGGER action_observations_immutable BEFORE UPDATE ON action_observations
BEGIN SELECT RAISE(ABORT, 'immutable observation'); END;
CREATE TRIGGER action_observations_retained BEFORE DELETE ON action_observations
BEGIN SELECT RAISE(ABORT, 'retain observation'); END;
CREATE TABLE action_measurements (
  observation_id TEXT NOT NULL REFERENCES action_observations(id),
  ordinal INTEGER NOT NULL,
  sample_json TEXT NOT NULL CHECK (json_valid(sample_json)),
  PRIMARY KEY (observation_id,ordinal)
) STRICT;
CREATE TRIGGER action_measurements_immutable BEFORE UPDATE ON action_measurements
BEGIN SELECT RAISE(ABORT, 'immutable measurement'); END;
CREATE TRIGGER action_measurements_retained BEFORE DELETE ON action_measurements
BEGIN SELECT RAISE(ABORT, 'retain measurement'); END;
CREATE TABLE action_retry_authorizations (
  action_id TEXT PRIMARY KEY REFERENCES actions(id),
  report_id TEXT NOT NULL REFERENCES action_reports(id),
  attempt_id TEXT NOT NULL REFERENCES business_attempts(id),
  consumed INTEGER NOT NULL CHECK (consumed IN (0,1))
) STRICT;
CREATE TABLE remote_run_links (
  action_id TEXT PRIMARY KEY REFERENCES actions(id),
  child_run_id TEXT NOT NULL UNIQUE REFERENCES runs(id)
) STRICT;
CREATE TRIGGER remote_run_link_immutable BEFORE UPDATE ON remote_run_links
WHEN NEW.action_id<>OLD.action_id OR NEW.child_run_id<>OLD.child_run_id
BEGIN SELECT RAISE(ABORT, 'immutable remote run link'); END;
CREATE TRIGGER remote_run_links_retained BEFORE DELETE ON remote_run_links
BEGIN SELECT RAISE(ABORT, 'retain remote run link'); END;
` },
  { version: 15, name: "durable-wait-wake", sql: `
ALTER TABLE waits ADD COLUMN resumed_frame_id TEXT REFERENCES frames(id);
CREATE TRIGGER wait_winner_immutable BEFORE UPDATE OF winner,wake_json ON waits
WHEN OLD.winner IS NOT NULL AND (NEW.winner IS NOT OLD.winner OR NEW.wake_json IS NOT OLD.wake_json)
BEGIN SELECT RAISE(ABORT, 'immutable wait winner'); END;
CREATE TRIGGER wait_resume_immutable BEFORE UPDATE OF resumed_frame_id ON waits
WHEN OLD.resumed_frame_id IS NOT NULL AND NEW.resumed_frame_id IS NOT OLD.resumed_frame_id
BEGIN SELECT RAISE(ABORT, 'immutable wait resume'); END;
CREATE TRIGGER frame_facts_immutable BEFORE UPDATE ON frame_facts
BEGIN SELECT RAISE(ABORT, 'immutable frame facts'); END;
CREATE TRIGGER frame_facts_retained BEFORE DELETE ON frame_facts
BEGIN SELECT RAISE(ABORT, 'retain frame facts'); END;
CREATE TABLE wake_jobs (
  id TEXT PRIMARY KEY REFERENCES waits(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  agent_id TEXT NOT NULL,
  agent_version TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','assigned','accepted')),
  assignment_id TEXT,
  assignment_epoch INTEGER NOT NULL DEFAULT 0,
  node_incarnation TEXT REFERENCES node_sessions(incarnation),
  assignment_deadline_at INTEGER,
  assignment_json TEXT CHECK (assignment_json IS NULL OR json_valid(assignment_json)),
  grant_json TEXT CHECK (grant_json IS NULL OR json_valid(grant_json)),
  lease_epoch INTEGER
) STRICT;
CREATE INDEX wake_jobs_ready ON wake_jobs(state,agent_id,agent_version,id);
CREATE TRIGGER wake_job_identity_immutable BEFORE UPDATE OF id,run_id,agent_id,agent_version,principal_id ON wake_jobs
BEGIN SELECT RAISE(ABORT, 'immutable wake job identity'); END;
CREATE TRIGGER wake_jobs_retained BEFORE DELETE ON wake_jobs
BEGIN SELECT RAISE(ABORT, 'retain wake job'); END;
CREATE INDEX work_items_run_wait_queue ON work_items(available_at,id) WHERE kind='run' AND stage IN ('waiting','wake');
` },
  { version: 16, name: "effective-cancellation-barriers", sql: `
ALTER TABLE runs ADD COLUMN parent_action_id TEXT;
UPDATE runs SET parent_action_id=json_extract(snapshot_json,'$.parentActionID');
CREATE TRIGGER run_parent_insert AFTER INSERT ON runs BEGIN
  UPDATE runs SET parent_action_id=json_extract(NEW.snapshot_json,'$.parentActionID') WHERE id=NEW.id;
END;
CREATE TRIGGER run_parent_immutable BEFORE UPDATE OF snapshot_json ON runs
WHEN json_extract(NEW.snapshot_json,'$.parentActionID') IS NOT OLD.parent_action_id
BEGIN SELECT RAISE(ABORT, 'immutable Run parent'); END;
CREATE TABLE cancellation_barriers (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL UNIQUE,
  scope TEXT NOT NULL CHECK (scope IN ('run','remaining_actions','action')),
  run_id TEXT NOT NULL REFERENCES runs(id),
  action_id TEXT REFERENCES actions(id),
  cancellation_json TEXT NOT NULL CHECK (json_valid(cancellation_json)),
  CHECK ((scope='action' AND action_id IS NOT NULL) OR (scope<>'action' AND action_id IS NULL)),
  FOREIGN KEY (action_id,run_id) REFERENCES actions(id,run_id)
) STRICT;
CREATE UNIQUE INDEX cancellation_run_scope ON cancellation_barriers(run_id,scope) WHERE scope<>'action';
CREATE UNIQUE INDEX cancellation_action_scope ON cancellation_barriers(action_id) WHERE scope='action';
CREATE INDEX cancellation_order ON cancellation_barriers(sequence,run_id,action_id);
CREATE TRIGGER cancellation_barriers_immutable BEFORE UPDATE ON cancellation_barriers
BEGIN SELECT RAISE(ABORT, 'immutable cancellation barrier'); END;
CREATE TRIGGER cancellation_barriers_retained BEFORE DELETE ON cancellation_barriers
BEGIN SELECT RAISE(ABORT, 'retain cancellation barrier'); END;
INSERT INTO cancellation_barriers(id,scope,run_id,action_id,cancellation_json)
  SELECT 'legacy.run:'||id,'run',id,NULL,json_extract(snapshot_json,'$.cancellation') FROM runs
  WHERE business_cancelled=1 AND json_type(snapshot_json,'$.cancellation')='object';
INSERT INTO cancellation_barriers(id,scope,run_id,action_id,cancellation_json)
  SELECT 'legacy.completion:'||id,'remaining_actions',id,NULL,
    CASE WHEN json_type(snapshot_json,'$.cancellation')='object' THEN json_extract(snapshot_json,'$.cancellation')
      ELSE json_object('source','completion','reason',NULL,'requestedAt',COALESCE(json_extract(snapshot_json,'$.completedAt'),0)) END
  FROM runs WHERE business_terminal=1;
INSERT INTO cancellation_barriers(id,scope,run_id,action_id,cancellation_json)
  SELECT 'legacy.action:'||id,'action',run_id,id,json_extract(snapshot_json,'$.cancellation') FROM actions
  WHERE business_cancelled=1 AND json_type(snapshot_json,'$.cancellation')='object';
INSERT INTO work_items(id,kind,subject_id,stage,revision,available_at,payload_json,result_json)
  SELECT json_array('cancel.cleanup',r.id),'cancel_cleanup',r.id,'pending',0,
    COALESCE(json_extract(r.snapshot_json,'$.completedAt'),0),json_object('runID',r.id,'barrierID',b.id),NULL
  FROM runs r JOIN cancellation_barriers b ON b.run_id=r.id AND b.scope='remaining_actions'
  WHERE EXISTS (SELECT 1 FROM actions a WHERE a.run_id=r.id AND a.outcome_json IS NULL);
` },
  { version: 17, name: "unique-child-run", sql: `
CREATE UNIQUE INDEX runs_unique_parent_action ON runs(parent_action_id) WHERE parent_action_id IS NOT NULL;
` },
  { version: 18, name: "resumable-dispatch-preparation", sql: `
DROP TRIGGER dispatch_preparation_identity_immutable;
CREATE TRIGGER dispatch_preparation_identity_immutable BEFORE UPDATE OF frame_id,policies_json,original_json ON dispatch_preparations
BEGIN SELECT RAISE(ABORT, 'immutable dispatch preparation'); END;
CREATE TRIGGER dispatch_preparation_round_guard BEFORE UPDATE OF round_id ON dispatch_preparations
WHEN NOT EXISTS (SELECT 1 FROM prepare_rounds old JOIN prepare_rounds next ON next.id=NEW.round_id
  WHERE old.id=OLD.round_id AND old.state='expired' AND next.state='pending'
    AND old.owner_kind='frame' AND next.owner_kind='frame' AND old.owner_id=OLD.frame_id AND next.owner_id=OLD.frame_id
    AND next.number=old.number+1)
BEGIN SELECT RAISE(ABORT, 'invalid dispatch preparation round'); END;
` },
  { version: 19, name: "recheck-management-identity", sql: `
CREATE TABLE action_recheck_requests (
  action_id TEXT PRIMARY KEY REFERENCES actions(id),
  principal_id TEXT NOT NULL,
  request_key TEXT NOT NULL,
  FOREIGN KEY(principal_id,request_key) REFERENCES commands(principal_id,request_key)
) STRICT;
CREATE TRIGGER action_recheck_requests_retained BEFORE DELETE ON action_recheck_requests
BEGIN SELECT RAISE(ABORT, 'retain recheck identity'); END;
` },
  { version: 20, name: "driver-coordinator-recovery", sql: `
DROP TRIGGER driver_job_identity_immutable;
CREATE TRIGGER driver_job_identity_immutable BEFORE UPDATE OF id,run_id,agent_id,agent_version,principal_id,input_json ON driver_jobs
BEGIN SELECT RAISE(ABORT, 'immutable driver job identity'); END;
CREATE TRIGGER driver_job_coordinator_recovery BEFORE UPDATE OF coordinator_epoch ON driver_jobs
WHEN NEW.coordinator_epoch<=OLD.coordinator_epoch OR NEW.coordinator_epoch<>(SELECT coordinator_epoch FROM domain WHERE singleton=1)
BEGIN SELECT RAISE(ABORT, 'invalid driver coordinator recovery'); END;
DROP TRIGGER finalize_job_identity_immutable;
CREATE TRIGGER finalize_job_identity_immutable BEFORE UPDATE OF id,run_id,agent_id,agent_version,principal_id,input_json ON finalize_jobs
BEGIN SELECT RAISE(ABORT, 'immutable finalize job identity'); END;
CREATE TRIGGER finalize_job_coordinator_recovery BEFORE UPDATE OF coordinator_epoch ON finalize_jobs
WHEN NEW.coordinator_epoch<=OLD.coordinator_epoch OR NEW.coordinator_epoch<>(SELECT coordinator_epoch FROM domain WHERE singleton=1)
BEGIN SELECT RAISE(ABORT, 'invalid finalize coordinator recovery'); END;
DROP TRIGGER start_job_identity_immutable;
CREATE TRIGGER start_job_identity_immutable BEFORE UPDATE OF id,agent_id,agent_version,principal_id ON start_jobs
BEGIN SELECT RAISE(ABORT, 'immutable start job identity'); END;
CREATE TRIGGER start_job_coordinator_recovery BEFORE UPDATE OF coordinator_epoch ON start_jobs
WHEN NEW.coordinator_epoch<=OLD.coordinator_epoch OR NEW.coordinator_epoch<>(SELECT coordinator_epoch FROM domain WHERE singleton=1)
BEGIN SELECT RAISE(ABORT, 'invalid start coordinator recovery'); END;
` },
  { version: 21, name: "fixed-capacity-reconciliation-support", sql: `
CREATE TABLE capacity_reconciliation_support (
  id TEXT NOT NULL,
  version TEXT NOT NULL,
  supported INTEGER NOT NULL CHECK(supported IN (0,1)),
  revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
  PRIMARY KEY(id,version)
) STRICT;
CREATE TRIGGER capacity_reconciliation_support_immutable BEFORE UPDATE OF id,version,supported ON capacity_reconciliation_support
BEGIN SELECT RAISE(ABORT, 'immutable Capacity reconciliation support'); END;
CREATE TRIGGER capacity_reconciliation_support_retained BEFORE DELETE ON capacity_reconciliation_support
BEGIN SELECT RAISE(ABORT, 'retain Capacity reconciliation support'); END;
INSERT INTO capacity_reconciliation_support(id,version,supported)
  SELECT json_extract(j.value,'$.ref.id'),json_extract(j.value,'$.ref.version'),
    CASE WHEN MIN(json_extract(j.value,'$.supportsReconcile'))=MAX(json_extract(j.value,'$.supportsReconcile'))
      THEN MIN(json_extract(j.value,'$.supportsReconcile')) ELSE 2 END
  FROM node_sessions s, json_each(s.configuration_json,'$.definitions') j
  WHERE json_extract(j.value,'$.kind')='capacity' AND json_type(j.value,'$.supportsReconcile') IN ('true','false')
  GROUP BY json_extract(j.value,'$.ref.id'),json_extract(j.value,'$.ref.version');
` },
  { version: 22, name: "response-limit-watermark", sql: `
CREATE TABLE response_limit_watermark (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  bytes INTEGER NOT NULL CHECK(bytes>=0)
) STRICT;
INSERT INTO response_limit_watermark(singleton,bytes) VALUES(1,0);
` },
  { version: 23, name: "node-claim-gate", sql: `
CREATE TABLE node_claim_gates (
  incarnation TEXT PRIMARY KEY REFERENCES node_sessions(incarnation),
  enabled INTEGER NOT NULL CHECK(enabled IN (0,1))
) STRICT;
INSERT INTO node_claim_gates(incarnation,enabled) SELECT incarnation,CASE WHEN state='online' THEN 1 ELSE 0 END FROM node_sessions;
` },
  { version: 24, name: "submit-response-budget", sql: `
ALTER TABLE submit_preparations ADD COLUMN response_budget INTEGER CHECK(response_budget IS NULL OR response_budget>=0);
CREATE TRIGGER submit_response_budget_immutable BEFORE UPDATE OF response_budget ON submit_preparations
BEGIN SELECT RAISE(ABORT, 'immutable submit response budget'); END;
` },
  { version: 25, name: "attempt-report-value-limit", sql: `
ALTER TABLE action_attempt_state ADD COLUMN report_value_limit INTEGER CHECK(report_value_limit IS NULL OR report_value_limit>0);
CREATE TRIGGER action_attempt_report_value_limit_immutable BEFORE UPDATE OF report_value_limit ON action_attempt_state
BEGIN SELECT RAISE(ABORT, 'immutable attempt report limit'); END;
` },
  { version: 26, name: "submit-fragmented-response", sql: `
ALTER TABLE submit_preparations ADD COLUMN fragmented_response INTEGER NOT NULL DEFAULT 0 CHECK(fragmented_response IN (0,1));
CREATE TRIGGER submit_fragmented_response_immutable BEFORE UPDATE OF fragmented_response ON submit_preparations
BEGIN SELECT RAISE(ABORT, 'immutable submit response capability'); END;
` },
  { version: 27, name: "chunked-values", sql: `
CREATE TABLE value_capacity (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>=0)
) STRICT;
INSERT INTO value_capacity VALUES(1,0);
CREATE TABLE value_manifests (
  id TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  encoding TEXT NOT NULL,
  total_bytes INTEGER NOT NULL CHECK(total_bytes>0),
  digest TEXT NOT NULL CHECK(length(digest)=64),
  written_bytes INTEGER NOT NULL DEFAULT 0 CHECK(written_bytes>=0 AND written_bytes<=total_bytes),
  revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
  state TEXT NOT NULL CHECK(state IN ('staging','sealed','published','deleting')),
  created_epoch INTEGER NOT NULL CHECK(created_epoch>0)
) STRICT;
CREATE TRIGGER value_manifest_identity_immutable BEFORE UPDATE OF id,owner,encoding,total_bytes,digest,created_epoch ON value_manifests
BEGIN SELECT RAISE(ABORT, 'immutable value identity'); END;
CREATE TRIGGER value_manifest_state_guard BEFORE UPDATE ON value_manifests
WHEN NOT ((OLD.state='staging' AND NEW.state IN ('staging','sealed','deleting'))
       OR (OLD.state='sealed' AND NEW.state IN ('sealed','published','deleting'))
       OR (OLD.state='published' AND NEW.state='published')
       OR (OLD.state='deleting' AND NEW.state='deleting'))
  OR (OLD.state<>'staging' AND NEW.written_bytes<>OLD.written_bytes)
BEGIN SELECT RAISE(ABORT, 'invalid value state transition'); END;
CREATE TRIGGER value_manifest_delete_guard BEFORE DELETE ON value_manifests
WHEN OLD.state<>'deleting' BEGIN SELECT RAISE(ABORT, 'value is not collecting'); END;
CREATE TABLE value_chunks (
  value_id TEXT NOT NULL REFERENCES value_manifests(id),
  offset_bytes INTEGER NOT NULL CHECK(offset_bytes>=0),
  length_bytes INTEGER NOT NULL CHECK(length_bytes>0 AND length_bytes<=65536),
  chunk_digest TEXT NOT NULL CHECK(length(chunk_digest)=64),
  payload_blob BLOB NOT NULL CHECK(length(payload_blob)=length_bytes),
  PRIMARY KEY(value_id,offset_bytes)
) STRICT;
CREATE TRIGGER value_chunk_insert_guard BEFORE INSERT ON value_chunks
WHEN NOT EXISTS (SELECT 1 FROM value_manifests WHERE id=NEW.value_id AND state='staging'
  AND written_bytes=NEW.offset_bytes+NEW.length_bytes)
BEGIN SELECT RAISE(ABORT, 'value chunk is not staged'); END;
CREATE TRIGGER value_chunk_no_update BEFORE UPDATE ON value_chunks BEGIN SELECT RAISE(ABORT, 'immutable value chunk'); END;
CREATE TRIGGER value_chunk_delete_guard BEFORE DELETE ON value_chunks
WHEN (SELECT state FROM value_manifests WHERE id=OLD.value_id)<>'deleting'
BEGIN SELECT RAISE(ABORT, 'value chunk is not collecting'); END;
CREATE TABLE value_references (
  value_id TEXT NOT NULL REFERENCES value_manifests(id),
  operation_id TEXT NOT NULL REFERENCES history(operation_id),
  PRIMARY KEY(value_id,operation_id)
) STRICT;
CREATE TRIGGER value_reference_no_update BEFORE UPDATE ON value_references BEGIN SELECT RAISE(ABORT, 'immutable value reference'); END;
CREATE TRIGGER value_reference_no_delete BEFORE DELETE ON value_references BEGIN SELECT RAISE(ABORT, 'retain published value reference'); END;
` },
  { version: 28, name: "action-report-value-reference", sql: `
ALTER TABLE action_reports ADD COLUMN report_value_id TEXT REFERENCES value_manifests(id);
CREATE TRIGGER action_report_value_insert_guard BEFORE INSERT ON action_reports
WHEN (NEW.report_value_id IS NULL AND NEW.report_json='null')
  OR (NEW.report_value_id IS NOT NULL AND (NEW.report_json<>'null'
    OR NOT EXISTS(SELECT 1 FROM value_manifests WHERE id=NEW.report_value_id AND state='published')))
BEGIN SELECT RAISE(ABORT, 'invalid action report storage'); END;
` },
  { version: 29, name: "report-transfer-bindings", sql: `
CREATE TABLE report_transfer_bindings (
  id TEXT PRIMARY KEY CHECK(length(id)=64),
  control_value_id TEXT NOT NULL REFERENCES value_manifests(id),
  report_value_id TEXT NOT NULL UNIQUE REFERENCES value_manifests(id),
  source_digest TEXT NOT NULL CHECK(length(source_digest)=64),
  worker_digest TEXT NOT NULL CHECK(length(worker_digest)=64),
  state TEXT NOT NULL CHECK(state IN ('active','published','deleting')),
  revision INTEGER NOT NULL CHECK(revision>=0),
  created_epoch INTEGER NOT NULL CHECK(created_epoch>0)
) STRICT;
CREATE TRIGGER report_transfer_insert_guard BEFORE INSERT ON report_transfer_bindings
WHEN NOT EXISTS(SELECT 1 FROM value_manifests WHERE id=NEW.control_value_id AND state='sealed')
  OR NOT EXISTS(SELECT 1 FROM value_manifests WHERE id=NEW.report_value_id AND state='staging' AND written_bytes=0)
BEGIN SELECT RAISE(ABORT, 'report transfer contents are not ready'); END;
CREATE TRIGGER report_transfer_identity_immutable BEFORE UPDATE OF id,control_value_id,report_value_id,source_digest,worker_digest,created_epoch ON report_transfer_bindings
BEGIN SELECT RAISE(ABORT, 'immutable report transfer identity'); END;
CREATE TRIGGER report_transfer_state_guard BEFORE UPDATE ON report_transfer_bindings
WHEN NOT (OLD.state='active' AND NEW.state IN ('active','published','deleting')
       OR OLD.state='published' AND NEW.state='published'
       OR OLD.state='deleting' AND NEW.state='deleting')
BEGIN SELECT RAISE(ABORT, 'invalid report transfer state'); END;
CREATE TRIGGER report_transfer_delete_guard BEFORE DELETE ON report_transfer_bindings
WHEN OLD.state<>'deleting' BEGIN SELECT RAISE(ABORT, 'report transfer is not collecting'); END;
` },
  { version: 30, name: "action-value-projections", sql: `
CREATE TRIGGER value_manifest_transfer_delete_guard BEFORE UPDATE OF state ON value_manifests
WHEN NEW.state='deleting' AND OLD.state<>'deleting' AND EXISTS (
  SELECT 1 FROM report_transfer_bindings WHERE state IN ('active','published') AND (control_value_id=OLD.id OR report_value_id=OLD.id)
)
BEGIN SELECT RAISE(ABORT, 'value is retained by report transfer'); END;
CREATE TABLE value_projections (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL REFERENCES actions(id),
  encoding TEXT NOT NULL CHECK(encoding='report-projection-v1'),
  binding_count INTEGER NOT NULL CHECK(binding_count>0),
  template_json TEXT NOT NULL CHECK(json_valid(template_json))
) STRICT;
CREATE TRIGGER value_projection_no_update BEFORE UPDATE ON value_projections BEGIN SELECT RAISE(ABORT, 'immutable value projection'); END;
CREATE TRIGGER value_projection_no_delete BEFORE DELETE ON value_projections BEGIN SELECT RAISE(ABORT, 'retain value projection'); END;
CREATE TABLE value_projection_bindings (
  projection_id TEXT NOT NULL REFERENCES value_projections(id),
  ordinal INTEGER NOT NULL CHECK(ordinal>=0),
  report_id TEXT NOT NULL REFERENCES action_reports(id),
  path_json TEXT NOT NULL CHECK(json_valid(path_json)),
  selector TEXT NOT NULL CHECK(selector IN ('report','result','value','evidence','measurements')),
  PRIMARY KEY(projection_id,ordinal)
) STRICT;
CREATE TRIGGER value_projection_binding_insert_guard BEFORE INSERT ON value_projection_bindings
WHEN NOT EXISTS (SELECT 1 FROM value_projections p JOIN action_reports r ON r.action_id=p.action_id WHERE p.id=NEW.projection_id AND r.id=NEW.report_id AND NEW.ordinal<p.binding_count)
BEGIN SELECT RAISE(ABORT, 'projection report source mismatch'); END;
CREATE TRIGGER value_projection_binding_no_update BEFORE UPDATE ON value_projection_bindings BEGIN SELECT RAISE(ABORT, 'immutable value projection binding'); END;
CREATE TRIGGER value_projection_binding_no_delete BEFORE DELETE ON value_projection_bindings BEGIN SELECT RAISE(ABORT, 'retain value projection binding'); END;
ALTER TABLE action_observations ADD COLUMN snapshot_projection_id TEXT REFERENCES value_projections(id);
CREATE TRIGGER action_observation_projection_insert_guard BEFORE INSERT ON action_observations
WHEN (NEW.snapshot_projection_id IS NULL AND NEW.snapshot_json='null') OR (NEW.snapshot_projection_id IS NOT NULL AND (
  NEW.source<>'execute' OR NEW.snapshot_json<>'null' OR NOT EXISTS (
    SELECT 1 FROM value_projections p WHERE p.id=NEW.snapshot_projection_id AND p.action_id=NEW.action_id
      AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id AND b.report_id=NEW.source_id)
  )))
BEGIN SELECT RAISE(ABORT, 'invalid action observation projection'); END;
ALTER TABLE history ADD COLUMN data_projection_id TEXT REFERENCES value_projections(id);
CREATE TRIGGER history_projection_insert_guard BEFORE INSERT ON history
WHEN NEW.data_projection_id IS NOT NULL AND (NEW.operation_id IS NOT NULL OR NEW.type<>'action.observed' OR NEW.data_json<>'null'
  OR NOT EXISTS (SELECT 1 FROM value_projections p WHERE p.id=NEW.data_projection_id AND p.action_id=NEW.subject_id
    AND json_extract(p.template_json,'$.actionID')=NEW.subject_id
    AND json_extract(p.template_json,'$.observation.source')='execute'
    AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id
      AND b.report_id=json_extract(p.template_json,'$.observation.id'))))
BEGIN SELECT RAISE(ABORT, 'invalid history projection'); END;
ALTER TABLE facts ADD COLUMN outcome_projection_id TEXT REFERENCES value_projections(id);
CREATE TRIGGER fact_projection_insert_guard BEFORE INSERT ON facts
WHEN (NEW.outcome_projection_id IS NULL AND NEW.outcome_json='null') OR (NEW.outcome_projection_id IS NOT NULL AND (
  NEW.outcome_json<>'null' OR NOT EXISTS (SELECT 1 FROM value_projections p JOIN value_projection_bindings b ON b.projection_id=p.id
    WHERE p.id=NEW.outcome_projection_id AND p.action_id=NEW.action_id AND p.binding_count=1 AND b.ordinal=0 AND b.path_json='[]' AND b.selector='result')))
BEGIN SELECT RAISE(ABORT, 'invalid fact projection'); END;
CREATE TABLE action_controls (
  action_id TEXT NOT NULL REFERENCES actions(id),
  revision INTEGER NOT NULL CHECK(revision>=0),
  operation_id TEXT NOT NULL REFERENCES history(operation_id) DEFERRABLE INITIALLY DEFERRED,
  control_json TEXT NOT NULL CHECK(json_valid(control_json)),
  PRIMARY KEY(action_id,revision)
) STRICT;
CREATE TRIGGER action_control_insert_guard BEFORE INSERT ON action_controls
WHEN NOT EXISTS (SELECT 1 FROM actions a WHERE a.id=NEW.action_id AND a.revision=NEW.revision
  AND json_extract(NEW.control_json,'$.encoding')='action-control-v1'
  AND json_extract(NEW.control_json,'$.id')=a.id AND json_extract(NEW.control_json,'$.runID')=a.run_id
  AND json_extract(NEW.control_json,'$.revision')=a.revision
  AND json_extract(NEW.control_json,'$.kind')=json_extract(a.request_json,'$.kind')
  AND json_extract(NEW.control_json,'$.target.id')=json_extract(a.request_json,'$.target.id')
  AND json_extract(NEW.control_json,'$.target.version')=json_extract(a.request_json,'$.target.version')
  AND json_extract(NEW.control_json,'$.status') IS json_extract(a.snapshot_json,'$.status')
  AND json_extract(NEW.control_json,'$.outcomeKind') IS json_extract(a.outcome_json,'$.kind')
  AND json_extract(NEW.control_json,'$.latestAttempt') IS json_extract(a.snapshot_json,'$.latestAttempt')
  AND json_extract(NEW.control_json,'$.executionCount') IS json_extract(a.snapshot_json,'$.executionCount')
  AND json_extract(NEW.control_json,'$.cancelRequested') IS json_extract(a.snapshot_json,'$.cancelRequested')
  AND json_extract(NEW.control_json,'$.recheckRequested') IS json_extract(a.snapshot_json,'$.recheckRequested')
  AND json_extract(NEW.control_json,'$.blockedCode') IS json_extract(a.snapshot_json,'$.blocked.code')
  AND json_extract(NEW.control_json,'$.outcomeObservationID') IS json_extract(a.snapshot_json,'$.outcomeObservationID')
  AND json_extract(NEW.control_json,'$.remoteRunID') IS json_extract(a.snapshot_json,'$.remoteRunID')
  AND json_extract(NEW.control_json,'$.updatedAt') IS json_extract(a.snapshot_json,'$.updatedAt')
  AND json_extract(NEW.control_json,'$.settledAt') IS json_extract(a.snapshot_json,'$.settledAt'))
BEGIN SELECT RAISE(ABORT, 'action control does not match committed snapshot'); END;
CREATE TRIGGER action_control_no_update BEFORE UPDATE ON action_controls BEGIN SELECT RAISE(ABORT, 'immutable action control'); END;
CREATE TRIGGER action_control_no_delete BEFORE DELETE ON action_controls BEGIN SELECT RAISE(ABORT, 'retain action control'); END;
` },
  { version: 31, name: "settled-report-history-projection", sql: `
DROP TRIGGER history_projection_insert_guard;
CREATE TRIGGER history_projection_insert_guard BEFORE INSERT ON history
WHEN NEW.data_projection_id IS NOT NULL AND (NEW.operation_id IS NOT NULL OR NEW.data_json<>'null'
  OR NOT EXISTS (SELECT 1 FROM value_projections p WHERE p.id=NEW.data_projection_id AND p.action_id=NEW.subject_id
    AND json_extract(p.template_json,'$.actionID')=NEW.subject_id AND (
      (NEW.type='action.observed' AND json_extract(p.template_json,'$.observation.source')='execute'
        AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id
          AND b.report_id=json_extract(p.template_json,'$.observation.id')))
      OR (NEW.type='action.settled' AND p.binding_count=1 AND json_type(p.template_json,'$.outcome')='null'
        AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
          JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
          JOIN action_observations o ON o.id=r.id AND o.action_id=r.action_id AND o.source='execute' AND o.source_id=r.id
          JOIN value_projections op ON op.id=o.snapshot_projection_id AND op.action_id=r.action_id
            AND json_extract(op.template_json,'$.acceptedAsOutcome')=1
          JOIN actions a ON a.id=r.action_id AND json_extract(a.outcome_json,'$.kind')='succeeded'
            AND json_extract(a.snapshot_json,'$.outcomeObservationID')=r.id
          WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["outcome"]' AND b.selector='result'
            AND r.phase='execute' AND r.report_value_id IS NOT NULL AND r.id=json_extract(p.template_json,'$.sourceObservationID')))
    )))
BEGIN SELECT RAISE(ABORT, 'invalid history projection'); END;
` },
  { version: 32, name: "action-outcome-projection", sql: `
ALTER TABLE actions ADD COLUMN outcome_projection_id TEXT REFERENCES value_projections(id);
CREATE TRIGGER action_outcome_projection_insert_guard BEFORE INSERT ON actions
WHEN NEW.outcome_projection_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'new action cannot have an outcome projection'); END;
CREATE TRIGGER action_outcome_projection_guard BEFORE UPDATE OF outcome_json,outcome_projection_id ON actions
WHEN (OLD.outcome_projection_id IS NOT NULL AND (NEW.outcome_projection_id IS NOT OLD.outcome_projection_id OR NEW.outcome_json IS NOT OLD.outcome_json))
  OR (NEW.outcome_projection_id IS NOT NULL AND (NEW.outcome_json<>'{"kind":"succeeded"}' OR NOT EXISTS (
    SELECT 1 FROM value_projections p JOIN value_projection_bindings b ON b.projection_id=p.id
      JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
      JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
    WHERE p.id=NEW.outcome_projection_id AND p.action_id=NEW.id AND p.template_json='null' AND p.binding_count=1
      AND b.ordinal=0 AND b.path_json='[]' AND b.selector='result' AND r.phase='execute' AND r.report_value_id IS NOT NULL
      AND r.authority_json IS NOT NULL AND json_extract(NEW.snapshot_json,'$.outcome.kind')='succeeded'
      AND json_extract(NEW.snapshot_json,'$.outcomeObservationID')=r.id
  )))
BEGIN SELECT RAISE(ABORT, 'invalid action outcome projection'); END;
` },
  { version: 33, name: "frame-input-references", sql: `
ALTER TABLE driver_jobs ADD COLUMN input_encoding TEXT NOT NULL DEFAULT 'inline' CHECK(input_encoding IN ('inline','frame-ref-v1'));
ALTER TABLE finalize_jobs ADD COLUMN input_encoding TEXT NOT NULL DEFAULT 'inline' CHECK(input_encoding IN ('inline','frame-ref-v1'));
CREATE TRIGGER driver_job_frame_input_insert_guard BEFORE INSERT ON driver_jobs
WHEN NEW.input_encoding='frame-ref-v1' AND (NEW.input_json<>'null' OR NOT EXISTS
  (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.state='open'))
BEGIN SELECT RAISE(ABORT, 'invalid driver frame input reference'); END;
CREATE TRIGGER finalize_job_frame_input_insert_guard BEFORE INSERT ON finalize_jobs
WHEN NEW.input_encoding='frame-ref-v1' AND (NEW.input_json<>'null' OR NOT EXISTS
  (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.state='open'))
BEGIN SELECT RAISE(ABORT, 'invalid finalize frame input reference'); END;
CREATE TRIGGER driver_job_input_encoding_immutable BEFORE UPDATE OF input_encoding ON driver_jobs
WHEN NEW.input_encoding IS NOT OLD.input_encoding BEGIN SELECT RAISE(ABORT, 'immutable driver input encoding'); END;
CREATE TRIGGER finalize_job_input_encoding_immutable BEFORE UPDATE OF input_encoding ON finalize_jobs
WHEN NEW.input_encoding IS NOT OLD.input_encoding BEGIN SELECT RAISE(ABORT, 'immutable finalize input encoding'); END;
` },
  { version: 34, name: "action-snapshot-discriminator", sql: `
ALTER TABLE actions ADD COLUMN snapshot_projection_id TEXT REFERENCES value_projections(id);
-- Existing synchronous consumers are being migrated to an explicit accessor.
-- Until every control and recovery path understands references, no durable
-- Action may publish one and expose an internal placeholder as business data.
CREATE TRIGGER action_snapshot_projection_insert_guard BEFORE INSERT ON actions
WHEN NEW.snapshot_projection_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'action snapshot reference is not enabled'); END;
CREATE TRIGGER action_snapshot_projection_update_guard BEFORE UPDATE OF snapshot_projection_id ON actions
WHEN NEW.snapshot_projection_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'action snapshot reference is not enabled'); END;
` },
  { version: 35, name: "frame-wait-references", sql: `
ALTER TABLE frames ADD COLUMN input_encoding TEXT NOT NULL DEFAULT 'inline' CHECK(input_encoding IN ('inline','wait-ref-v1'));
ALTER TABLE frames ADD COLUMN input_operation_id TEXT REFERENCES history(operation_id) DEFERRABLE INITIALLY DEFERRED;
CREATE TRIGGER frame_wait_reference_insert_guard BEFORE INSERT ON frames
WHEN (NEW.input_encoding='inline' AND NEW.input_operation_id IS NOT NULL)
 OR (NEW.input_encoding='wait-ref-v1' AND (NEW.input_operation_id IS NULL
   OR json_extract(NEW.input_json,'$.encoding') IS NOT 'frame-wait-ref-v1'
   OR json_extract(NEW.input_json,'$.operationID') IS NOT NEW.input_operation_id
   OR json_extract(NEW.input_json,'$.input.frameID') IS NOT NEW.id
   OR json_extract(NEW.input_json,'$.input.run.id') IS NOT NEW.run_id
   OR json_type(NEW.input_json,'$.input.wake') IS NOT 'null'
   OR json_type(NEW.input_json,'$.length') IS NOT 'integer' OR json_extract(NEW.input_json,'$.length')<=0
   OR json_type(NEW.input_json,'$.digest') IS NOT 'text' OR length(json_extract(NEW.input_json,'$.digest'))<>64
   OR json_type(NEW.input_json,'$.waitID') IS NOT 'text'
   OR EXISTS (SELECT 1 FROM history h WHERE h.operation_id=NEW.input_operation_id)))
BEGIN SELECT RAISE(ABORT, 'invalid referenced frame input'); END;
CREATE TRIGGER frame_wait_reference_encoding_immutable BEFORE UPDATE OF input_encoding,input_operation_id ON frames
WHEN NEW.input_encoding IS NOT OLD.input_encoding OR NEW.input_operation_id IS NOT OLD.input_operation_id
BEGIN SELECT RAISE(ABORT, 'immutable frame input reference'); END;
CREATE TRIGGER frame_wait_reference_receipt_guard AFTER INSERT ON history
WHEN EXISTS (SELECT 1 FROM frames f WHERE f.input_operation_id=NEW.operation_id AND
  (NEW.type<>'core.resume' OR NEW.subject_id<>f.run_id OR json_extract(NEW.request_json,'$.frameID') IS NOT f.id
   OR json_extract(NEW.request_json,'$.inputRef') IS NOT json(f.input_json)))
BEGIN SELECT RAISE(ABORT, 'frame reference receipt mismatch'); END;
` },
  { version: 36, name: "frame-assignment-references", sql: `
ALTER TABLE driver_jobs ADD COLUMN assignment_encoding TEXT NOT NULL DEFAULT 'inline' CHECK(assignment_encoding IN ('inline','frame-ref-v1'));
ALTER TABLE finalize_jobs ADD COLUMN assignment_encoding TEXT NOT NULL DEFAULT 'inline' CHECK(assignment_encoding IN ('inline','frame-ref-v1'));
CREATE TRIGGER driver_assignment_reference_insert_guard BEFORE INSERT ON driver_jobs
WHEN NEW.assignment_encoding='frame-ref-v1' AND (NEW.assignment_json IS NULL
  OR json_extract(NEW.assignment_json,'$.encoding') IS NOT 'frame-assignment-v1'
  OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
  OR json_extract(NEW.assignment_json,'$.assignment.driver') IS NOT 1
  OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id
  OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
  OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
  OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
  OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
  OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
  OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='wait-ref-v1'
    AND f.input_operation_id=json_extract(NEW.assignment_json,'$.frameOperationID')))
BEGIN SELECT RAISE(ABORT, 'invalid referenced driver assignment'); END;
CREATE TRIGGER finalize_assignment_reference_insert_guard BEFORE INSERT ON finalize_jobs
WHEN NEW.assignment_encoding='frame-ref-v1' AND (NEW.assignment_json IS NULL
  OR json_extract(NEW.assignment_json,'$.encoding') IS NOT 'frame-assignment-v1'
  OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
  OR json_extract(NEW.assignment_json,'$.assignment.finalize') IS NOT 1
  OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id
  OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
  OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
  OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
  OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
  OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
  OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='wait-ref-v1'
    AND f.input_operation_id=json_extract(NEW.assignment_json,'$.frameOperationID')))
BEGIN SELECT RAISE(ABORT, 'invalid referenced finalize assignment'); END;
CREATE TRIGGER driver_assignment_reference_guard BEFORE UPDATE ON driver_jobs
WHEN NEW.assignment_encoding='frame-ref-v1' AND (NEW.assignment_json IS NULL
  OR json_extract(NEW.assignment_json,'$.encoding') IS NOT 'frame-assignment-v1'
  OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
  OR json_extract(NEW.assignment_json,'$.assignment.driver') IS NOT 1
  OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id
  OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
  OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
  OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
  OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
  OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
  OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='wait-ref-v1'
    AND f.input_operation_id=json_extract(NEW.assignment_json,'$.frameOperationID')))
BEGIN SELECT RAISE(ABORT, 'invalid referenced driver assignment'); END;
CREATE TRIGGER finalize_assignment_reference_guard BEFORE UPDATE ON finalize_jobs
WHEN NEW.assignment_encoding='frame-ref-v1' AND (NEW.assignment_json IS NULL
  OR json_extract(NEW.assignment_json,'$.encoding') IS NOT 'frame-assignment-v1'
  OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
  OR json_extract(NEW.assignment_json,'$.assignment.finalize') IS NOT 1
  OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id
  OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
  OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
  OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
  OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
  OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
  OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='wait-ref-v1'
    AND f.input_operation_id=json_extract(NEW.assignment_json,'$.frameOperationID')))
BEGIN SELECT RAISE(ABORT, 'invalid referenced finalize assignment'); END;
CREATE TRIGGER frame_assignment_claim_receipt_guard AFTER INSERT ON history
WHEN NEW.type='validation.claimed' AND json_extract(NEW.result_json,'$.assignment.encoding')='frame-assignment-v1'
 AND NOT EXISTS (SELECT 1 FROM driver_jobs j WHERE j.assignment_encoding='frame-ref-v1' AND j.id=json_extract(NEW.result_json,'$.assignment.frameID')
     AND json(j.assignment_json) IS json_extract(NEW.result_json,'$.assignment')
   UNION ALL SELECT 1 FROM finalize_jobs j WHERE j.assignment_encoding='frame-ref-v1' AND j.id=json_extract(NEW.result_json,'$.assignment.frameID')
     AND json(j.assignment_json) IS json_extract(NEW.result_json,'$.assignment'))
BEGIN SELECT RAISE(ABORT, 'frame assignment receipt mismatch'); END;
` },
  { version: 37, name: "wait-fact-history-projection", sql: `
DROP TRIGGER history_projection_insert_guard;
CREATE TRIGGER history_projection_insert_guard BEFORE INSERT ON history
WHEN NEW.data_projection_id IS NOT NULL AND (NEW.operation_id IS NOT NULL OR NEW.data_json<>'null'
  OR NOT EXISTS (SELECT 1 FROM value_projections p WHERE p.id=NEW.data_projection_id AND (
    (p.action_id=NEW.subject_id AND json_extract(p.template_json,'$.actionID')=NEW.subject_id AND (
      (NEW.type='action.observed' AND json_extract(p.template_json,'$.observation.source')='execute'
        AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id AND b.report_id=json_extract(p.template_json,'$.observation.id')))
      OR (NEW.type='action.settled' AND p.binding_count=1 AND json_type(p.template_json,'$.outcome')='null'
        AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
          JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
          JOIN action_observations o ON o.id=r.id AND o.action_id=r.action_id AND o.source='execute' AND o.source_id=r.id
          JOIN value_projections op ON op.id=o.snapshot_projection_id AND op.action_id=r.action_id AND json_extract(op.template_json,'$.acceptedAsOutcome')=1
          JOIN actions a ON a.id=r.action_id AND json_extract(a.outcome_json,'$.kind')='succeeded' AND json_extract(a.snapshot_json,'$.outcomeObservationID')=r.id
          WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["outcome"]' AND b.selector='result'
            AND r.phase='execute' AND r.report_value_id IS NOT NULL AND r.id=json_extract(p.template_json,'$.sourceObservationID')))))
    OR (NEW.type='wait.fact' AND p.binding_count=1 AND json_type(p.template_json,'$.fact.outcome')='null'
      AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN actions a ON a.id=p.action_id
        JOIN facts f ON f.id=json_extract(p.template_json,'$.fact.id') AND f.action_id=a.id AND f.run_id=a.run_id
        JOIN value_projection_bindings fb ON fb.projection_id=f.outcome_projection_id AND fb.ordinal=0
        WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["fact","outcome"]' AND b.selector='result'
          AND fb.report_id=b.report_id AND fb.path_json='[]' AND fb.selector='result'
          AND a.run_id=NEW.subject_id AND json_extract(p.template_json,'$.fact.actionID')=a.id
          AND json_extract(p.template_json,'$.fact.actionKey')=a.action_key))
  )))
BEGIN SELECT RAISE(ABORT, 'invalid history projection'); END;
` },
  { version: 38, name: "report-diagnostic-transfer-receipts", sql: `
CREATE TABLE report_transfer_diagnostics (
  id TEXT PRIMARY KEY CHECK(length(id)=64),
  control_value_id TEXT NOT NULL REFERENCES value_manifests(id),
  report_value_id TEXT NOT NULL UNIQUE,
  report_owner TEXT NOT NULL,
  report_encoding TEXT NOT NULL CHECK(report_encoding='utf8-v1'),
  digest_scope TEXT NOT NULL CHECK(digest_scope='canonical-report-envelope-utf8-v1'),
  report_length INTEGER NOT NULL CHECK(report_length>0),
  report_digest TEXT NOT NULL CHECK(length(report_digest)=64),
  source_digest TEXT NOT NULL CHECK(length(source_digest)=64),
  worker_digest TEXT NOT NULL CHECK(length(worker_digest)=64),
  action_id TEXT NOT NULL, attempt_id TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('execute','reconcile')),
  report_id TEXT NOT NULL UNIQUE REFERENCES action_reports(id),
  operation_id TEXT NOT NULL UNIQUE REFERENCES history(operation_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TRIGGER report_transfer_diagnostic_insert_guard BEFORE INSERT ON report_transfer_diagnostics
WHEN NOT EXISTS (SELECT 1 FROM value_manifests c WHERE c.id=NEW.control_value_id AND c.state='published' AND c.digest=NEW.source_digest)
 OR NOT EXISTS (SELECT 1 FROM value_manifests r WHERE r.id=NEW.report_value_id AND r.owner=NEW.report_owner AND r.encoding=NEW.report_encoding
    AND r.total_bytes=NEW.report_length AND r.digest=NEW.report_digest AND r.state='deleting')
 OR EXISTS (SELECT 1 FROM report_transfer_bindings b WHERE b.id=NEW.id OR b.report_value_id=NEW.report_value_id)
 OR EXISTS (SELECT 1 FROM value_references v WHERE v.value_id=NEW.report_value_id)
 OR NOT EXISTS (SELECT 1 FROM action_reports a WHERE a.id=NEW.report_id AND a.action_id=NEW.action_id AND a.attempt_id=NEW.attempt_id
    AND a.phase=NEW.phase AND a.report_value_id IS NULL AND json_extract(a.report_json,'$.kind')='diagnostic'
    AND json_extract(a.report_json,'$.code')='RESULT_CONTRACT_VIOLATION' AND json_extract(a.report_json,'$.digest')=NEW.report_digest)
BEGIN SELECT RAISE(ABORT, 'invalid diagnostic transfer receipt'); END;
CREATE TRIGGER report_transfer_diagnostic_receipt_guard AFTER INSERT ON history
WHEN EXISTS (SELECT 1 FROM report_transfer_diagnostics d WHERE d.operation_id=NEW.operation_id AND
  (NEW.type<>'action.report_received' OR NEW.subject_id<>d.action_id
   OR json_extract(NEW.request_json,'$.actionID') IS NOT d.action_id OR json_extract(NEW.request_json,'$.attemptID') IS NOT d.attempt_id
   OR json_extract(NEW.request_json,'$.phase') IS NOT d.phase OR json_extract(NEW.request_json,'$.reportID') IS NOT d.report_id
   OR json_extract(NEW.request_json,'$.reportDigest') IS NOT d.report_digest OR json_extract(NEW.request_json,'$.reportBytes') IS NOT d.report_length
   OR json_extract(NEW.request_json,'$.reportDigestScope') IS NOT d.digest_scope
   OR json_extract(NEW.request_json,'$.controlValue.id') IS NOT d.control_value_id OR json_extract(NEW.request_json,'$.controlValue.digest') IS NOT d.source_digest
   OR json_extract(NEW.request_json,'$.transfer.id') IS NOT d.id OR json_extract(NEW.request_json,'$.transfer.sourceDigest') IS NOT d.source_digest
   OR json_extract(NEW.request_json,'$.transfer.workerDigest') IS NOT d.worker_digest
   OR json_extract(NEW.result_json,'$.diagnostic') IS NOT 1 OR json_extract(NEW.result_json,'$.stored') IS NOT 0))
BEGIN SELECT RAISE(ABORT, 'diagnostic report receipt mismatch'); END;
CREATE TRIGGER report_transfer_diagnostic_immutable BEFORE UPDATE ON report_transfer_diagnostics
BEGIN SELECT RAISE(ABORT, 'immutable diagnostic transfer receipt'); END;
CREATE TRIGGER report_transfer_diagnostic_retained BEFORE DELETE ON report_transfer_diagnostics
BEGIN SELECT RAISE(ABORT, 'retain diagnostic transfer receipt'); END;
` },
  { version: 39, name: "report-direct-diagnostic-receipts", sql: `
CREATE TABLE report_direct_diagnostics (
  operation_id TEXT PRIMARY KEY REFERENCES history(operation_id) DEFERRABLE INITIALLY DEFERRED,
  report_id TEXT NOT NULL UNIQUE REFERENCES action_reports(id),
  report_value_id TEXT NOT NULL UNIQUE, report_owner TEXT NOT NULL, report_encoding TEXT NOT NULL CHECK(report_encoding='utf8-v1'),
  report_length INTEGER NOT NULL CHECK(report_length>0), report_digest TEXT NOT NULL CHECK(length(report_digest)=64),
  action_id TEXT NOT NULL, attempt_id TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('execute','reconcile'))
) STRICT;
CREATE TRIGGER report_direct_diagnostic_insert_guard BEFORE INSERT ON report_direct_diagnostics
WHEN NOT EXISTS (SELECT 1 FROM value_manifests r WHERE r.id=NEW.report_value_id AND r.owner=NEW.report_owner AND r.encoding=NEW.report_encoding
  AND r.total_bytes=NEW.report_length AND r.digest=NEW.report_digest AND r.state='deleting')
 OR EXISTS (SELECT 1 FROM report_transfer_bindings b WHERE b.report_value_id=NEW.report_value_id OR b.control_value_id=NEW.report_value_id)
 OR EXISTS (SELECT 1 FROM value_references v WHERE v.value_id=NEW.report_value_id)
 OR NOT EXISTS (SELECT 1 FROM action_reports a WHERE a.id=NEW.report_id AND a.action_id=NEW.action_id AND a.attempt_id=NEW.attempt_id
  AND a.phase=NEW.phase AND a.report_value_id IS NULL AND a.authority_json IS NULL AND json_extract(a.report_json,'$.kind')='diagnostic'
  AND json_extract(a.report_json,'$.digest')=NEW.report_digest)
BEGIN SELECT RAISE(ABORT, 'invalid direct diagnostic receipt'); END;
CREATE TRIGGER report_direct_diagnostic_receipt_guard AFTER INSERT ON history
WHEN EXISTS (SELECT 1 FROM report_direct_diagnostics d WHERE d.operation_id=NEW.operation_id AND
 (NEW.type<>'action.report_received' OR NEW.subject_id<>d.action_id OR json_extract(NEW.request_json,'$.actionID') IS NOT d.action_id
 OR json_extract(NEW.request_json,'$.attemptID') IS NOT d.attempt_id OR json_extract(NEW.request_json,'$.phase') IS NOT d.phase
 OR json_extract(NEW.request_json,'$.reportID') IS NOT d.report_id OR json_extract(NEW.request_json,'$.reportDigest') IS NOT d.report_digest
 OR json_extract(NEW.request_json,'$.reportBytes') IS NOT d.report_length OR json_extract(NEW.request_json,'$.reportOwner') IS NOT d.report_owner
 OR json_extract(NEW.request_json,'$.reportEncoding') IS NOT d.report_encoding OR json_extract(NEW.result_json,'$.diagnostic') IS NOT 1
 OR json_extract(NEW.result_json,'$.stored') IS NOT 0))
BEGIN SELECT RAISE(ABORT, 'direct diagnostic receipt mismatch'); END;
CREATE TRIGGER report_direct_diagnostic_immutable BEFORE UPDATE ON report_direct_diagnostics BEGIN SELECT RAISE(ABORT, 'immutable direct diagnostic'); END;
CREATE TRIGGER report_direct_diagnostic_retained BEFORE DELETE ON report_direct_diagnostics BEGIN SELECT RAISE(ABORT, 'retain direct diagnostic'); END;
CREATE TRIGGER report_direct_no_active_bound_collection BEFORE UPDATE OF state ON value_manifests
WHEN NEW.state='deleting' AND EXISTS (SELECT 1 FROM report_transfer_bindings b WHERE (b.report_value_id=NEW.id OR b.control_value_id=NEW.id) AND b.state<>'deleting')
BEGIN SELECT RAISE(ABORT, 'cannot collect active bound value'); END;
` },
  { version: 40, name: "successful-capacity-snapshots", sql: `
-- Only an already settled Capacity outcome can use this representation.
-- Nonterminal, cancelled and remote Actions retain the original write paths.
DROP TRIGGER action_snapshot_projection_update_guard;
CREATE TRIGGER action_snapshot_projection_update_guard BEFORE UPDATE OF snapshot_json,snapshot_projection_id,revision,request_json,outcome_json,outcome_projection_id ON actions
WHEN (NEW.snapshot_projection_id IS NULL AND (OLD.snapshot_projection_id IS NOT NULL OR NEW.snapshot_json='null'))
 OR (NEW.snapshot_projection_id IS NOT NULL AND (NEW.snapshot_json<>'null'
  OR json_extract(NEW.request_json,'$.kind') IS NOT 'capacity' OR NEW.outcome_json IS NOT '{"kind":"succeeded"}'
  OR NEW.outcome_projection_id IS NULL OR NOT EXISTS (
    SELECT 1 FROM value_projections p WHERE p.id=NEW.snapshot_projection_id AND p.action_id=NEW.id
      AND json_extract(p.template_json,'$.id') IS NEW.id AND json_extract(p.template_json,'$.runID') IS NEW.run_id
      AND json_extract(p.template_json,'$.revision') IS NEW.revision AND json_extract(p.template_json,'$.status') IS 'settled'
      AND json_extract(p.template_json,'$.cancelRequested') IS 0 AND json_type(p.template_json,'$.outcome') IS 'null'
      AND json_extract(p.template_json,'$.request') IS json(NEW.request_json)
      AND json_extract(p.template_json,'$.outcomeObservationID') IS NOT NULL
      AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id)
  )))
BEGIN SELECT RAISE(ABORT, 'invalid successful Action snapshot reference'); END;
DROP TRIGGER action_control_insert_guard;
CREATE TRIGGER action_control_insert_guard BEFORE INSERT ON action_controls
WHEN NOT EXISTS (SELECT 1 FROM actions a LEFT JOIN value_projections p ON p.id=a.snapshot_projection_id AND p.action_id=a.id WHERE a.id=NEW.action_id AND a.revision=NEW.revision
  AND json_extract(NEW.control_json,'$.encoding')='action-control-v1'
  AND json_extract(NEW.control_json,'$.id')=a.id AND json_extract(NEW.control_json,'$.runID')=a.run_id
  AND json_extract(NEW.control_json,'$.revision')=a.revision
  AND json_extract(NEW.control_json,'$.kind')=json_extract(a.request_json,'$.kind')
  AND json_extract(NEW.control_json,'$.target.id')=json_extract(a.request_json,'$.target.id')
  AND json_extract(NEW.control_json,'$.target.version')=json_extract(a.request_json,'$.target.version')
  AND json_extract(NEW.control_json,'$.status') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.status')
  AND json_extract(NEW.control_json,'$.outcomeKind') IS json_extract(a.outcome_json,'$.kind')
  AND json_extract(NEW.control_json,'$.latestAttempt') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.latestAttempt')
  AND json_extract(NEW.control_json,'$.executionCount') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.executionCount')
  AND json_extract(NEW.control_json,'$.cancelRequested') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.cancelRequested')
  AND json_extract(NEW.control_json,'$.recheckRequested') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.recheckRequested')
  AND json_extract(NEW.control_json,'$.blockedCode') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.blocked.code')
  AND json_extract(NEW.control_json,'$.outcomeObservationID') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.outcomeObservationID')
  AND json_extract(NEW.control_json,'$.remoteRunID') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.remoteRunID')
  AND json_extract(NEW.control_json,'$.updatedAt') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.updatedAt')
  AND json_extract(NEW.control_json,'$.settledAt') IS json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE p.template_json END,'$.settledAt'))
BEGIN SELECT RAISE(ABORT, 'action control does not match committed snapshot'); END;
DROP TRIGGER action_outcome_projection_guard;
CREATE TRIGGER action_outcome_projection_guard BEFORE UPDATE OF outcome_json,outcome_projection_id ON actions
WHEN (OLD.outcome_projection_id IS NOT NULL AND (NEW.outcome_projection_id IS NOT OLD.outcome_projection_id OR NEW.outcome_json IS NOT OLD.outcome_json))
  OR (NEW.outcome_projection_id IS NOT NULL AND (NEW.outcome_json<>'{"kind":"succeeded"}' OR NOT EXISTS (
    SELECT 1 FROM value_projections p JOIN value_projection_bindings b ON b.projection_id=p.id
      JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
      JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
    WHERE p.id=NEW.outcome_projection_id AND p.action_id=NEW.id AND p.template_json='null' AND p.binding_count=1
      AND b.ordinal=0 AND b.path_json='[]' AND b.selector='result' AND r.phase='execute' AND r.report_value_id IS NOT NULL
      AND r.authority_json IS NOT NULL AND (NEW.snapshot_projection_id IS NOT NULL OR json_extract(NEW.snapshot_json,'$.outcome.kind')='succeeded')
      AND json_extract(CASE WHEN NEW.snapshot_projection_id IS NULL THEN NEW.snapshot_json ELSE (SELECT template_json FROM value_projections WHERE id=NEW.snapshot_projection_id AND action_id=NEW.id) END,'$.outcomeObservationID')=r.id
  )))
BEGIN SELECT RAISE(ABORT, 'invalid action outcome projection'); END;
DROP TRIGGER action_observation_projection_insert_guard;
CREATE TRIGGER action_observation_projection_insert_guard BEFORE INSERT ON action_observations
WHEN (NEW.snapshot_projection_id IS NULL AND NEW.snapshot_json='null') OR (NEW.snapshot_projection_id IS NOT NULL AND (
  NEW.source NOT IN ('execute','reconcile') OR NEW.snapshot_json<>'null' OR NOT EXISTS (
    SELECT 1 FROM value_projections p WHERE p.id=NEW.snapshot_projection_id AND p.action_id=NEW.action_id
      AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id AND b.report_id=NEW.source_id)
  )))
BEGIN SELECT RAISE(ABORT, 'invalid action observation projection'); END;
DROP TRIGGER history_projection_insert_guard;
CREATE TRIGGER history_projection_insert_guard BEFORE INSERT ON history
WHEN NEW.data_projection_id IS NOT NULL AND (NEW.operation_id IS NOT NULL OR NEW.data_json<>'null'
  OR NOT EXISTS (SELECT 1 FROM value_projections p WHERE p.id=NEW.data_projection_id AND (
    (p.action_id=NEW.subject_id AND json_extract(p.template_json,'$.actionID')=NEW.subject_id AND (
      (NEW.type='action.observed' AND json_extract(p.template_json,'$.observation.source') IN ('execute','reconcile')
        AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id AND b.report_id=json_extract(p.template_json,'$.observation.id')))
      OR (NEW.type='action.settled' AND p.binding_count=1 AND json_type(p.template_json,'$.outcome')='null'
        AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
          JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
          JOIN action_observations o ON o.id=r.id AND o.action_id=r.action_id AND o.source='execute' AND o.source_id=r.id
          JOIN value_projections op ON op.id=o.snapshot_projection_id AND op.action_id=r.action_id AND json_extract(op.template_json,'$.acceptedAsOutcome')=1
          JOIN actions a ON a.id=r.action_id AND json_extract(a.outcome_json,'$.kind')='succeeded' AND json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE (SELECT template_json FROM value_projections WHERE id=a.snapshot_projection_id AND action_id=a.id) END,'$.outcomeObservationID')=r.id
          WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["outcome"]' AND b.selector='result'
            AND r.phase='execute' AND r.report_value_id IS NOT NULL AND r.id=json_extract(p.template_json,'$.sourceObservationID')))))
    OR (NEW.type='wait.fact' AND p.binding_count=1 AND json_type(p.template_json,'$.fact.outcome')='null'
      AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN actions a ON a.id=p.action_id
        JOIN facts f ON f.id=json_extract(p.template_json,'$.fact.id') AND f.action_id=a.id AND f.run_id=a.run_id
        JOIN value_projection_bindings fb ON fb.projection_id=f.outcome_projection_id AND fb.ordinal=0
        WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["fact","outcome"]' AND b.selector='result'
          AND fb.report_id=b.report_id AND fb.path_json='[]' AND fb.selector='result'
          AND a.run_id=NEW.subject_id AND json_extract(p.template_json,'$.fact.actionID')=a.id
          AND json_extract(p.template_json,'$.fact.actionKey')=a.action_key))
  )))
BEGIN SELECT RAISE(ABORT, 'invalid history projection'); END;
CREATE TRIGGER action_snapshot_receipt_guard AFTER INSERT ON history
WHEN EXISTS (SELECT 1 FROM action_controls c JOIN actions a ON a.id=c.action_id AND a.revision=c.revision
 WHERE c.operation_id=NEW.operation_id AND a.snapshot_projection_id IS NOT NULL AND
 (NEW.type<>'report.observed' OR NEW.subject_id<>a.id
  OR json_extract(NEW.request_json,'$.version') IS NOT 'report-observation-ref-v1'
  OR json_extract(NEW.request_json,'$.snapshotProjectionID') IS NOT a.snapshot_projection_id
  OR json_extract(NEW.request_json,'$.actionControl') IS NOT json(c.control_json)))
BEGIN SELECT RAISE(ABORT, 'Action snapshot receipt mismatch'); END;
` },
  { version: 41, name: "action-field-snapshots", sql: `
-- Snapshot and observation may be written in either order. The deferred FK
-- prevents an acknowledged transaction from leaving a dangling field source.
ALTER TABLE actions ADD COLUMN snapshot_observation_id TEXT GENERATED ALWAYS AS (
  CASE WHEN json_extract(snapshot_json,'$.snapshotFormat')='action-fields-v1'
    THEN json_extract(snapshot_json,'$.latestObservation.id') END
) VIRTUAL REFERENCES action_observations(id) DEFERRABLE INITIALLY DEFERRED;
CREATE TRIGGER action_field_snapshot_insert_guard BEFORE INSERT ON actions
WHEN json_type(NEW.snapshot_json,'$.snapshotFormat') IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'new Action must have its original inline snapshot'); END;
CREATE TRIGGER action_field_snapshot_update_guard BEFORE UPDATE OF snapshot_json,outcome_json ON actions
WHEN json_type(NEW.snapshot_json,'$.snapshotFormat') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.snapshotFormat') IS NOT 'action-fields-v1'
 OR NEW.snapshot_projection_id IS NOT NULL
 OR json_extract(NEW.snapshot_json,'$.id') IS NOT NEW.id
 OR json_extract(NEW.snapshot_json,'$.runID') IS NOT NEW.run_id
 OR json_extract(NEW.snapshot_json,'$.revision') IS NOT NEW.revision
 OR json_extract(NEW.snapshot_json,'$.request') IS NOT json(NEW.request_json)
 OR json_extract(NEW.snapshot_json,'$.outcome.kind') IS NOT json_extract(NEW.outcome_json,'$.kind')
 OR (json_type(NEW.snapshot_json,'$.latestObservation') IS NOT 'null' AND
   (json_type(NEW.snapshot_json,'$.latestObservation.id') IS NOT 'text'
    OR EXISTS (SELECT 1 FROM action_observations o WHERE o.id=json_extract(NEW.snapshot_json,'$.latestObservation.id') AND o.action_id<>NEW.id)))
)
BEGIN SELECT RAISE(ABORT, 'invalid Action field snapshot'); END;
` },
  { version: 42, name: "capacity-result-projections", sql: `
-- Both execute and reconcile.settled supply CapacityResult. Cancellation is
-- still a management outcome and can never be adopted from a report value.
DROP TRIGGER action_outcome_projection_guard;
CREATE TRIGGER action_outcome_projection_guard BEFORE UPDATE OF outcome_json,outcome_projection_id ON actions
WHEN (OLD.outcome_projection_id IS NOT NULL AND (NEW.outcome_projection_id IS NOT OLD.outcome_projection_id OR NEW.outcome_json IS NOT OLD.outcome_json))
  OR (NEW.outcome_projection_id IS NOT NULL AND (NEW.outcome_json NOT IN ('{"kind":"succeeded"}','{"kind":"failed"}') OR NOT EXISTS (
    SELECT 1 FROM value_projections p JOIN value_projection_bindings b ON b.projection_id=p.id
      JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
      JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
    WHERE p.id=NEW.outcome_projection_id AND p.action_id=NEW.id AND p.template_json='null' AND p.binding_count=1
      AND b.ordinal=0 AND b.path_json='[]' AND b.selector='result' AND r.phase IN ('execute','reconcile') AND r.report_value_id IS NOT NULL
      AND r.authority_json IS NOT NULL AND (NEW.snapshot_projection_id IS NOT NULL OR json_extract(NEW.snapshot_json,'$.outcome.kind')=json_extract(NEW.outcome_json,'$.kind'))
      AND json_extract(CASE WHEN NEW.snapshot_projection_id IS NULL THEN NEW.snapshot_json ELSE (SELECT template_json FROM value_projections WHERE id=NEW.snapshot_projection_id AND action_id=NEW.id) END,'$.outcomeObservationID')=r.id
  )))
BEGIN SELECT RAISE(ABORT, 'invalid action outcome projection'); END;
DROP TRIGGER history_projection_insert_guard;
CREATE TRIGGER history_projection_insert_guard BEFORE INSERT ON history
WHEN NEW.data_projection_id IS NOT NULL AND (NEW.operation_id IS NOT NULL OR NEW.data_json<>'null'
  OR NOT EXISTS (SELECT 1 FROM value_projections p WHERE p.id=NEW.data_projection_id AND (
    (p.action_id=NEW.subject_id AND json_extract(p.template_json,'$.actionID')=NEW.subject_id AND (
      (NEW.type='action.observed' AND json_extract(p.template_json,'$.observation.source') IN ('execute','reconcile')
        AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id AND b.report_id=json_extract(p.template_json,'$.observation.id')))
      OR (NEW.type='action.settled' AND p.binding_count=1 AND json_type(p.template_json,'$.outcome')='null'
        AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
          JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
          JOIN action_observations o ON o.id=r.id AND o.action_id=r.action_id AND o.source=r.phase AND o.source_id=r.id
          JOIN value_projections op ON op.id=o.snapshot_projection_id AND op.action_id=r.action_id AND json_extract(op.template_json,'$.acceptedAsOutcome')=1
          JOIN actions a ON a.id=r.action_id AND json_extract(a.outcome_json,'$.kind') IN ('succeeded','failed') AND json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE (SELECT template_json FROM value_projections WHERE id=a.snapshot_projection_id AND action_id=a.id) END,'$.outcomeObservationID')=r.id
          WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["outcome"]' AND b.selector='result'
            AND r.phase IN ('execute','reconcile') AND r.report_value_id IS NOT NULL AND r.id=json_extract(p.template_json,'$.sourceObservationID')))))
    OR (NEW.type='wait.fact' AND p.binding_count=1 AND json_type(p.template_json,'$.fact.outcome')='null'
      AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN actions a ON a.id=p.action_id
        JOIN facts f ON f.id=json_extract(p.template_json,'$.fact.id') AND f.action_id=a.id AND f.run_id=a.run_id
        JOIN value_projection_bindings fb ON fb.projection_id=f.outcome_projection_id AND fb.ordinal=0
        WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["fact","outcome"]' AND b.selector='result'
          AND fb.report_id=b.report_id AND fb.path_json='[]' AND fb.selector='result'
          AND a.run_id=NEW.subject_id AND json_extract(p.template_json,'$.fact.actionID')=a.id
          AND json_extract(p.template_json,'$.fact.actionKey')=a.action_key))
  )))
BEGIN SELECT RAISE(ABORT, 'invalid history projection'); END;
` },
  { version: 43, name: "completed-frame-result-projections", sql: `
CREATE TABLE frame_result_projections (
  projection_id TEXT PRIMARY KEY REFERENCES value_projections(id),
  run_id TEXT NOT NULL REFERENCES runs(id),
  frame_id TEXT NOT NULL REFERENCES frames(id),
  operation_id TEXT NOT NULL REFERENCES history(operation_id) DEFERRABLE INITIALLY DEFERRED,
  digest TEXT NOT NULL CHECK(length(digest)=64 AND digest NOT GLOB '*[^a-f0-9]*'),
  paths_json TEXT NOT NULL CHECK(json_valid(paths_json) AND json_type(paths_json)='array' AND json_array_length(paths_json)>0)
) STRICT;
CREATE TRIGGER frame_result_projection_insert_guard BEFORE INSERT ON frame_result_projections
WHEN NOT EXISTS (SELECT 1 FROM value_projections p JOIN frames f ON f.id=NEW.frame_id AND f.run_id=NEW.run_id
 JOIN remote_run_links l ON l.child_run_id=NEW.run_id AND l.action_id=p.action_id
 WHERE p.id=NEW.projection_id AND p.binding_count=json_array_length(NEW.paths_json))
 OR EXISTS (SELECT 1 FROM value_projection_bindings WHERE projection_id=NEW.projection_id)
BEGIN SELECT RAISE(ABORT, 'invalid completed frame projection'); END;
CREATE TRIGGER frame_result_projection_no_update BEFORE UPDATE ON frame_result_projections BEGIN SELECT RAISE(ABORT, 'immutable frame projection'); END;
CREATE TRIGGER frame_result_projection_no_delete BEFORE DELETE ON frame_result_projections BEGIN SELECT RAISE(ABORT, 'retain frame projection'); END;
CREATE TRIGGER frame_result_receipt_guard AFTER INSERT ON history
WHEN EXISTS (SELECT 1 FROM frame_result_projections p WHERE p.operation_id=NEW.operation_id AND
 (NEW.type<>'core.complete' OR NEW.subject_id<>p.run_id OR json_extract(NEW.request_json,'$.frameID') IS NOT p.frame_id
 OR json_extract(NEW.request_json,'$.proposal.operationID') IS NOT NEW.operation_id
 OR json_extract(NEW.request_json,'$.proposal.after.state') IS NOT 'completed'))
BEGIN SELECT RAISE(ABORT, 'frame projection receipt mismatch'); END;
CREATE TRIGGER value_projection_exclusive_source BEFORE INSERT ON value_projection_bindings
WHEN EXISTS (SELECT 1 FROM frame_result_projections WHERE projection_id=NEW.projection_id)
BEGIN SELECT RAISE(ABORT, 'mixed projection source'); END;
DROP TRIGGER action_outcome_projection_guard;
CREATE TRIGGER action_outcome_projection_guard BEFORE UPDATE OF outcome_json,outcome_projection_id ON actions
WHEN ((OLD.outcome_projection_id IS NOT NULL AND (NEW.outcome_projection_id IS NOT OLD.outcome_projection_id OR NEW.outcome_json IS NOT OLD.outcome_json))
  OR (NEW.outcome_projection_id IS NOT NULL AND (NEW.outcome_json NOT IN ('{"kind":"succeeded"}','{"kind":"failed"}') OR NOT EXISTS (
    SELECT 1 FROM value_projections p JOIN value_projection_bindings b ON b.projection_id=p.id
      JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
      JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
    WHERE p.id=NEW.outcome_projection_id AND p.action_id=NEW.id AND p.template_json='null' AND p.binding_count=1
      AND b.ordinal=0 AND b.path_json='[]' AND b.selector='result' AND r.phase IN ('execute','reconcile') AND r.report_value_id IS NOT NULL
      AND r.authority_json IS NOT NULL AND (NEW.snapshot_projection_id IS NOT NULL OR json_extract(NEW.snapshot_json,'$.outcome.kind')=json_extract(NEW.outcome_json,'$.kind'))
      AND json_extract(CASE WHEN NEW.snapshot_projection_id IS NULL THEN NEW.snapshot_json ELSE (SELECT template_json FROM value_projections WHERE id=NEW.snapshot_projection_id AND action_id=NEW.id) END,'$.outcomeObservationID')=r.id
  )))) AND NOT (NEW.snapshot_projection_id IS NULL AND NEW.outcome_json IN ('{"kind":"succeeded"}','{"kind":"failed"}','{"kind":"cancelled"}')
 AND (OLD.outcome_projection_id IS NULL OR (NEW.outcome_projection_id IS OLD.outcome_projection_id AND NEW.outcome_json IS OLD.outcome_json))
 AND json_extract(NEW.snapshot_json,'$.outcome.kind')=json_extract(NEW.outcome_json,'$.kind')
 AND EXISTS (SELECT 1 FROM value_projections p JOIN frame_result_projections r ON r.projection_id=p.id
 WHERE p.id=NEW.outcome_projection_id AND p.action_id=NEW.id AND p.template_json='null' AND r.paths_json='[[]]'))
BEGIN SELECT RAISE(ABORT, 'invalid action outcome projection'); END;
DROP TRIGGER action_observation_projection_insert_guard;
CREATE TRIGGER action_observation_projection_insert_guard BEFORE INSERT ON action_observations
WHEN ((NEW.snapshot_projection_id IS NULL AND NEW.snapshot_json='null') OR (NEW.snapshot_projection_id IS NOT NULL AND (
  NEW.source NOT IN ('execute','reconcile') OR NEW.snapshot_json<>'null' OR NOT EXISTS (
    SELECT 1 FROM value_projections p WHERE p.id=NEW.snapshot_projection_id AND p.action_id=NEW.action_id
      AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id AND b.report_id=NEW.source_id)
  )))) AND NOT (NEW.source='remote' AND NEW.snapshot_json='null'
 AND EXISTS (SELECT 1 FROM value_projections p JOIN frame_result_projections r ON r.projection_id=p.id
 WHERE p.id=NEW.snapshot_projection_id AND p.action_id=NEW.action_id AND r.run_id=NEW.source_id
 AND r.paths_json='[["result"],["reported","payload"]]' AND json_extract(p.template_json,'$.id')=NEW.id
 AND json_extract(p.template_json,'$.source')='remote'
 AND json_extract(p.template_json,'$.evidence.operationID')=r.operation_id))
BEGIN SELECT RAISE(ABORT, 'invalid action observation projection'); END;
DROP TRIGGER fact_projection_insert_guard;
CREATE TRIGGER fact_projection_insert_guard BEFORE INSERT ON facts
WHEN ((NEW.outcome_projection_id IS NULL AND NEW.outcome_json='null') OR (NEW.outcome_projection_id IS NOT NULL AND (
  NEW.outcome_json<>'null' OR NOT EXISTS (SELECT 1 FROM value_projections p JOIN value_projection_bindings b ON b.projection_id=p.id
    WHERE p.id=NEW.outcome_projection_id AND p.action_id=NEW.action_id AND p.binding_count=1 AND b.ordinal=0 AND b.path_json='[]' AND b.selector='result')))) AND NOT (NEW.outcome_json='null' AND EXISTS (SELECT 1 FROM value_projections p JOIN frame_result_projections r ON r.projection_id=p.id
 JOIN actions a ON a.id=p.action_id WHERE p.id=NEW.outcome_projection_id AND p.action_id=NEW.action_id AND a.run_id=NEW.run_id
 AND p.template_json='null' AND r.paths_json='[[]]'))
BEGIN SELECT RAISE(ABORT, 'invalid fact projection'); END;
DROP TRIGGER history_projection_insert_guard;
CREATE TRIGGER history_projection_insert_guard BEFORE INSERT ON history
WHEN (NEW.data_projection_id IS NOT NULL AND (NEW.operation_id IS NOT NULL OR NEW.data_json<>'null'
  OR NOT EXISTS (SELECT 1 FROM value_projections p WHERE p.id=NEW.data_projection_id AND (
    (p.action_id=NEW.subject_id AND json_extract(p.template_json,'$.actionID')=NEW.subject_id AND (
      (NEW.type='action.observed' AND json_extract(p.template_json,'$.observation.source') IN ('execute','reconcile')
        AND p.binding_count=(SELECT count(*) FROM value_projection_bindings b WHERE b.projection_id=p.id AND b.report_id=json_extract(p.template_json,'$.observation.id')))
      OR (NEW.type='action.settled' AND p.binding_count=1 AND json_type(p.template_json,'$.outcome')='null'
        AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN action_reports r ON r.id=b.report_id AND r.action_id=p.action_id
          JOIN report_processing rp ON rp.report_id=r.id AND rp.state='done'
          JOIN action_observations o ON o.id=r.id AND o.action_id=r.action_id AND o.source=r.phase AND o.source_id=r.id
          JOIN value_projections op ON op.id=o.snapshot_projection_id AND op.action_id=r.action_id AND json_extract(op.template_json,'$.acceptedAsOutcome')=1
          JOIN actions a ON a.id=r.action_id AND json_extract(a.outcome_json,'$.kind') IN ('succeeded','failed') AND json_extract(CASE WHEN a.snapshot_projection_id IS NULL THEN a.snapshot_json ELSE (SELECT template_json FROM value_projections WHERE id=a.snapshot_projection_id AND action_id=a.id) END,'$.outcomeObservationID')=r.id
          WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["outcome"]' AND b.selector='result'
            AND r.phase IN ('execute','reconcile') AND r.report_value_id IS NOT NULL AND r.id=json_extract(p.template_json,'$.sourceObservationID')))))
    OR (NEW.type='wait.fact' AND p.binding_count=1 AND json_type(p.template_json,'$.fact.outcome')='null'
      AND EXISTS (SELECT 1 FROM value_projection_bindings b JOIN actions a ON a.id=p.action_id
        JOIN facts f ON f.id=json_extract(p.template_json,'$.fact.id') AND f.action_id=a.id AND f.run_id=a.run_id
        JOIN value_projection_bindings fb ON fb.projection_id=f.outcome_projection_id AND fb.ordinal=0
        WHERE b.projection_id=p.id AND b.ordinal=0 AND b.path_json='["fact","outcome"]' AND b.selector='result'
          AND fb.report_id=b.report_id AND fb.path_json='[]' AND fb.selector='result'
          AND a.run_id=NEW.subject_id AND json_extract(p.template_json,'$.fact.actionID')=a.id
          AND json_extract(p.template_json,'$.fact.actionKey')=a.action_key))
  )))) AND NOT (NEW.operation_id IS NULL AND NEW.data_json='null'
 AND EXISTS (SELECT 1 FROM value_projections p JOIN frame_result_projections r ON r.projection_id=p.id
 WHERE p.id=NEW.data_projection_id AND (
 (p.action_id=NEW.subject_id AND json_extract(p.template_json,'$.actionID')=p.action_id AND (
   (NEW.type='action.observed' AND r.paths_json='[["observation","result"],["observation","reported","payload"]]'
    AND EXISTS (SELECT 1 FROM action_observations o JOIN frame_result_projections op ON op.projection_id=o.snapshot_projection_id
      WHERE o.id=json_extract(p.template_json,'$.observation.id') AND o.action_id=p.action_id AND o.source='remote'
      AND o.source_id=r.run_id AND op.operation_id=r.operation_id AND op.frame_id=r.frame_id))
   OR (NEW.type='action.settled' AND r.paths_json='[["outcome"]]'
    AND EXISTS (SELECT 1 FROM actions a JOIN frame_result_projections ap ON ap.projection_id=a.outcome_projection_id
      JOIN action_observations o ON o.id=json_extract(p.template_json,'$.sourceObservationID') AND o.action_id=a.id
      JOIN value_projections op ON op.id=o.snapshot_projection_id
      WHERE a.id=p.action_id AND ap.operation_id=r.operation_id AND ap.frame_id=r.frame_id
      AND json_extract(op.template_json,'$.acceptedAsOutcome')=1))))
 OR (NEW.type='wait.fact' AND r.paths_json='[["fact","outcome"]]'
    AND EXISTS (SELECT 1 FROM facts f JOIN actions a ON a.id=f.action_id JOIN frame_result_projections fp ON fp.projection_id=f.outcome_projection_id
      WHERE f.id=json_extract(p.template_json,'$.fact.id') AND f.action_id=p.action_id AND f.run_id=NEW.subject_id
      AND json_extract(p.template_json,'$.fact.actionID')=a.id AND json_extract(p.template_json,'$.fact.actionKey')=a.action_key
      AND fp.operation_id=r.operation_id AND fp.frame_id=r.frame_id))
 )))
BEGIN SELECT RAISE(ABORT, 'invalid history projection'); END;
` },
  { version: 44, name: "run-outcome-snapshots", sql: `
ALTER TABLE runs ADD COLUMN outcome_frame_id TEXT GENERATED ALWAYS AS (
 CASE WHEN json_extract(snapshot_json,'$.snapshotFormat')='run-outcome-ref-v1' THEN json_extract(snapshot_json,'$.outcomeSource.frameID') END
) VIRTUAL REFERENCES frames(id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE runs ADD COLUMN outcome_operation_id TEXT GENERATED ALWAYS AS (
 CASE WHEN json_extract(snapshot_json,'$.snapshotFormat')='run-outcome-ref-v1' THEN json_extract(snapshot_json,'$.outcomeSource.operationID') END
) VIRTUAL REFERENCES history(operation_id) DEFERRABLE INITIALLY DEFERRED;
CREATE TRIGGER run_snapshot_reference_insert_guard BEFORE INSERT ON runs
WHEN json_type(NEW.snapshot_json,'$.snapshotFormat') IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'new Run requires an original snapshot'); END;
CREATE TRIGGER run_snapshot_reference_update_guard BEFORE UPDATE OF snapshot_json,core_json ON runs
WHEN json_type(NEW.snapshot_json,'$.snapshotFormat') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.snapshotFormat') IS NOT 'run-outcome-ref-v1'
 OR json_extract(NEW.snapshot_json,'$.id') IS NOT NEW.id
 OR json_extract(NEW.snapshot_json,'$.lifecycle') IS NOT 'completed'
 OR json_extract(NEW.core_json,'$.state') IS NOT 'completed'
 OR json_extract(NEW.snapshot_json,'$.outcome.kind') IS NOT json_extract(NEW.core_json,'$.outcome')
 OR json_extract(NEW.snapshot_json,'$.revision') IS NOT NEW.revision
 OR json_type(NEW.snapshot_json,'$.outcomeSource.digest') IS NOT 'text'
 OR length(json_extract(NEW.snapshot_json,'$.outcomeSource.digest'))<>64
 OR NEW.outcome_operation_id IS NULL
 OR NOT EXISTS (SELECT 1 FROM frames f JOIN frame_candidates c ON c.frame_id=f.id WHERE f.id=NEW.outcome_frame_id AND f.run_id=NEW.id)
)
BEGIN SELECT RAISE(ABORT, 'invalid Run outcome source'); END;
CREATE TRIGGER run_snapshot_reference_receipt_guard AFTER INSERT ON history
WHEN EXISTS (SELECT 1 FROM runs r WHERE r.outcome_operation_id=NEW.operation_id AND
 (NEW.type<>'core.complete' OR NEW.subject_id<>r.id OR json_extract(NEW.request_json,'$.frameID') IS NOT r.outcome_frame_id
 OR json_extract(NEW.request_json,'$.proposal.after') IS NOT json(r.core_json)))
BEGIN SELECT RAISE(ABORT, 'Run outcome receipt mismatch'); END;
` },
  { version: 45, name: "run-result-history-references", sql: `
ALTER TABLE history ADD COLUMN result_frame_id TEXT GENERATED ALWAYS AS (
 CASE WHEN json_extract(data_json,'$.encoding')='run-result-history-v1' THEN json_extract(data_json,'$.outcomeSource.frameID') END
) VIRTUAL REFERENCES frames(id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE history ADD COLUMN result_operation_id TEXT GENERATED ALWAYS AS (
 CASE WHEN json_extract(data_json,'$.encoding')='run-result-history-v1' THEN json_extract(data_json,'$.outcomeSource.operationID') END
) VIRTUAL REFERENCES history(operation_id) DEFERRABLE INITIALLY DEFERRED;
CREATE TRIGGER run_result_history_insert_guard BEFORE INSERT ON history
WHEN json_extract(NEW.data_json,'$.encoding')='run-result-history-v1' AND (
 NEW.operation_id IS NOT NULL OR NEW.data_projection_id IS NOT NULL
 OR NEW.type NOT IN ('run.completed','run.decision_committed')
 OR NEW.result_frame_id IS NULL OR NEW.result_operation_id IS NULL
 OR json_extract(NEW.data_json,'$.frameID') IS NOT NEW.result_frame_id
 OR (NEW.type='run.completed' AND json_type(NEW.data_json,'$.outcome') IS NOT 'null')
 OR (NEW.type='run.decision_committed' AND (json_extract(NEW.data_json,'$.decision.kind') IS NOT 'complete' OR json_type(NEW.data_json,'$.decision.outcome') IS NOT 'null'))
 OR NOT EXISTS (SELECT 1 FROM runs r JOIN history h ON h.operation_id=NEW.result_operation_id AND h.type='core.complete' AND h.subject_id=r.id
  WHERE r.id=NEW.subject_id AND r.outcome_frame_id=NEW.result_frame_id AND r.outcome_operation_id=NEW.result_operation_id
   AND json_extract(r.snapshot_json,'$.outcomeSource.digest')=json_extract(NEW.data_json,'$.outcomeSource.digest')
   AND json_extract(h.request_json,'$.frameID')=NEW.result_frame_id AND h.request_json=NEW.request_json)
)
BEGIN SELECT RAISE(ABORT, 'invalid Run history source'); END;
` },
  { version: 46, name: "driver-conclusion-values", sql: `
ALTER TABLE driver_jobs ADD COLUMN source_value_id TEXT GENERATED ALWAYS AS (
 CASE WHEN json_extract(report_json,'$.encoding')='driver-conclusion-ref-v1' THEN json_extract(report_json,'$.value.id') END
) VIRTUAL REFERENCES value_manifests(id);
ALTER TABLE frame_candidates ADD COLUMN source_value_id TEXT GENERATED ALWAYS AS (
 CASE WHEN json_extract(decision_json,'$.encoding')='driver-conclusion-ref-v1' THEN json_extract(decision_json,'$.value.id') END
) VIRTUAL REFERENCES value_manifests(id);
CREATE TRIGGER driver_value_guard BEFORE UPDATE OF report_json ON driver_jobs
WHEN json_extract(NEW.report_json,'$.encoding')='driver-conclusion-ref-v1' AND
 (NEW.source_value_id IS NULL OR NEW.state<>'accepted' OR json_extract(NEW.report_json,'$.frameID') IS NOT NEW.id)
BEGIN SELECT RAISE(ABORT, 'invalid Driver value source'); END;
CREATE TRIGGER candidate_value_guard BEFORE INSERT ON frame_candidates
WHEN json_extract(NEW.decision_json,'$.encoding')='driver-conclusion-ref-v1' AND NOT EXISTS
 (SELECT 1 FROM driver_jobs j JOIN value_manifests m ON m.id=j.source_value_id
  WHERE j.id=NEW.frame_id AND j.state='accepted' AND j.report_json=NEW.decision_json AND m.state='published')
BEGIN SELECT RAISE(ABORT, 'unaccepted Driver decision source'); END;
CREATE TRIGGER driver_value_receipt_guard AFTER INSERT ON history
WHEN NEW.type='driver.reported' AND json_type(NEW.request_json,'$.conclusionSource') IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM driver_jobs j JOIN value_manifests m ON m.id=j.source_value_id WHERE j.id=NEW.subject_id AND j.state='accepted' AND m.state='published'
  AND json_extract(NEW.request_json,'$.conclusionSource')=json(j.report_json) AND json_type(NEW.request_json,'$.conclusion')='null')
BEGIN SELECT RAISE(ABORT, 'Driver value receipt mismatch'); END;
` },
  { version: 47, name: "driver-staging-ownership", sql: `
CREATE TABLE driver_value_staging (
 value_id TEXT PRIMARY KEY REFERENCES value_manifests(id) ON DELETE CASCADE,
 frame_id TEXT NOT NULL REFERENCES driver_jobs(id),
 assignment_id TEXT NOT NULL
) STRICT;
CREATE TRIGGER driver_staging_origin_guard BEFORE INSERT ON driver_value_staging
WHEN NOT EXISTS (SELECT 1 FROM driver_jobs j WHERE j.id=NEW.frame_id AND j.state='assigned' AND j.assignment_id=NEW.assignment_id)
BEGIN SELECT RAISE(ABORT, 'Driver staging assignment is not current'); END;
CREATE TRIGGER driver_staging_no_update BEFORE UPDATE ON driver_value_staging
BEGIN SELECT RAISE(ABORT, 'immutable Driver staging ownership'); END;
` },
  { version: 48, name: "dispatch-preparation-references", sql: `
CREATE TRIGGER preparation_reference_insert BEFORE INSERT ON dispatch_preparations
WHEN json_type(NEW.original_json)<>'array' AND
 (json_extract(NEW.original_json,'$.encoding') IS NOT 'dispatch-originals-ref-v1'
  OR json_extract(NEW.original_json,'$.frameID') IS NOT NEW.frame_id
  OR NOT EXISTS (SELECT 1 FROM frame_candidates WHERE frame_id=NEW.frame_id))
BEGIN SELECT RAISE(ABORT, 'invalid preparation original source'); END;
CREATE TRIGGER preparation_reference_update BEFORE UPDATE OF rewritten_json,resolved_json,round_id,generation ON dispatch_preparations
WHEN (NEW.rewritten_json IS NOT NULL AND json_type(NEW.rewritten_json)<>'array' AND
 (json_extract(NEW.rewritten_json,'$.frameID') IS NOT NEW.frame_id OR
  (json_extract(NEW.rewritten_json,'$.encoding') IS NOT 'dispatch-originals-ref-v1' AND
   (json_extract(NEW.rewritten_json,'$.encoding') IS NOT 'dispatch-rewrites-ref-v1' OR json_extract(NEW.rewritten_json,'$.roundID') IS NOT NEW.round_id))))
 OR (NEW.resolved_json IS NOT NULL AND json_type(NEW.resolved_json)<>'array' AND
 (json_extract(NEW.resolved_json,'$.encoding') IS NOT 'dispatch-resolved-ref-v1'
  OR json_extract(NEW.resolved_json,'$.frameID') IS NOT NEW.frame_id
  OR json_extract(NEW.resolved_json,'$.roundID') IS NOT NEW.round_id
  OR json_extract(NEW.resolved_json,'$.generation') IS NOT NEW.generation))
BEGIN SELECT RAISE(ABORT, 'invalid preparation result source'); END;
CREATE TRIGGER dispatch_history_source_guard BEFORE INSERT ON history
WHEN json_extract(NEW.data_json,'$.encoding')='run-dispatch-history-v1' AND (
 NEW.operation_id IS NOT NULL OR NEW.data_projection_id IS NOT NULL OR NEW.type<>'run.decision_committed'
 OR json_type(NEW.data_json,'$.decision') IS NOT 'null' OR json_type(NEW.data_json,'$.digest') IS NOT 'text'
 OR NOT EXISTS (SELECT 1 FROM frames f JOIN frame_candidates c ON c.frame_id=f.id
  JOIN history h ON h.operation_id=json_extract(NEW.data_json,'$.operationID') AND h.type='core.suspend' AND h.subject_id=f.run_id
  WHERE f.id=json_extract(NEW.data_json,'$.frameID') AND f.run_id=NEW.subject_id AND f.state='committed'
   AND h.request_json=NEW.request_json AND json_extract(h.request_json,'$.frameID')=f.id)
)
BEGIN SELECT RAISE(ABORT, 'invalid dispatch history source'); END;
` },
  { version: 49, name: "action-request-snapshot-sources", sql: `
DROP TRIGGER action_field_snapshot_update_guard;
CREATE TRIGGER action_field_snapshot_update_guard BEFORE UPDATE OF snapshot_json,outcome_json ON actions
WHEN json_type(NEW.snapshot_json,'$.snapshotFormat') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.snapshotFormat') IS NOT 'action-fields-v1'
 OR NEW.snapshot_projection_id IS NOT NULL
 OR json_extract(NEW.snapshot_json,'$.id') IS NOT NEW.id
 OR json_extract(NEW.snapshot_json,'$.runID') IS NOT NEW.run_id
 OR json_extract(NEW.snapshot_json,'$.revision') IS NOT NEW.revision
 OR (json_type(NEW.snapshot_json,'$.requestSource') IS NULL AND json_extract(NEW.snapshot_json,'$.request') IS NOT json(NEW.request_json))
 OR json_extract(NEW.snapshot_json,'$.outcome.kind') IS NOT json_extract(NEW.outcome_json,'$.kind')
 OR (json_type(NEW.snapshot_json,'$.latestObservation') IS NOT 'null' AND
   (json_type(NEW.snapshot_json,'$.latestObservation.id') IS NOT 'text'
    OR EXISTS (SELECT 1 FROM action_observations o WHERE o.id=json_extract(NEW.snapshot_json,'$.latestObservation.id') AND o.action_id<>NEW.id)))
)
BEGIN SELECT RAISE(ABORT, 'invalid Action field snapshot'); END;
CREATE TRIGGER action_request_insert_guard BEFORE INSERT ON actions
WHEN json_type(NEW.snapshot_json,'$.requestSource') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.requestSource.encoding') IS NOT 'action-requests-v1'
 OR json_type(NEW.snapshot_json,'$.requestSource.digest') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.original.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.original.config') IS NOT 'null'
 OR json_type(NEW.snapshot_json,'$.request.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.request.config') IS NOT 'null'
 OR json_type(NEW.snapshot_json,'$.request.policies') IS NOT 'null'
 OR json_extract(NEW.snapshot_json,'$.original.key') IS NOT NEW.action_key
 OR json_extract(NEW.snapshot_json,'$.request.key') IS NOT NEW.action_key
 OR json_extract(NEW.snapshot_json,'$.request.target') IS NOT json_extract(NEW.request_json,'$.target')
 OR json_extract(NEW.snapshot_json,'$.request.kind') IS NOT json_extract(NEW.request_json,'$.kind')
)
BEGIN SELECT RAISE(ABORT, 'invalid Action request snapshot'); END;
CREATE TRIGGER action_request_update_guard BEFORE UPDATE OF snapshot_json ON actions
WHEN json_type(NEW.snapshot_json,'$.requestSource') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.requestSource.encoding') IS NOT 'action-requests-v1'
 OR json_type(NEW.snapshot_json,'$.requestSource.digest') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.original.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.original.config') IS NOT 'null'
 OR json_type(NEW.snapshot_json,'$.request.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.request.config') IS NOT 'null'
 OR json_type(NEW.snapshot_json,'$.request.policies') IS NOT 'null'
 OR json_extract(NEW.snapshot_json,'$.original.key') IS NOT NEW.action_key
 OR json_extract(NEW.snapshot_json,'$.request.key') IS NOT NEW.action_key
 OR json_extract(NEW.snapshot_json,'$.request.target') IS NOT json_extract(NEW.request_json,'$.target')
 OR json_extract(NEW.snapshot_json,'$.request.kind') IS NOT json_extract(NEW.request_json,'$.kind')
)
BEGIN SELECT RAISE(ABORT, 'invalid Action request snapshot'); END;
CREATE TRIGGER action_acceptance_source_guard BEFORE INSERT ON history
WHEN json_extract(NEW.data_json,'$.encoding')='action-request-history-v1' AND (
 NEW.type<>'action.accepted' OR NEW.operation_id IS NOT NULL OR NEW.data_projection_id IS NOT NULL
 OR json_extract(NEW.data_json,'$.actionID') IS NOT NEW.subject_id
 OR NOT EXISTS (SELECT 1 FROM actions a JOIN frames f ON f.run_id=a.run_id
   JOIN history h ON h.type='core.suspend' AND h.subject_id=a.run_id AND h.request_json=NEW.request_json
   WHERE a.id=NEW.subject_id AND f.id=json_extract(NEW.data_json,'$.frameID') AND f.state='committed'
   AND json_extract(h.request_json,'$.frameID')=f.id AND a.run_id=json_extract(NEW.data_json,'$.runID')
   AND a.action_key=json_extract(NEW.data_json,'$.key')
   AND json_extract(a.snapshot_json,'$.requestSource.digest')=json_extract(NEW.data_json,'$.digest'))
)
BEGIN SELECT RAISE(ABORT, 'invalid Action acceptance source'); END;
CREATE TRIGGER validation_report_source_guard BEFORE INSERT ON history
WHEN json_extract(NEW.request_json,'$.encoding')='validation-report-ref-v1' AND (
 NEW.type<>'validation.reported' OR NEW.operation_id IS NULL
 OR json_type(NEW.request_json,'$.assignment.payload') IS NOT 'null'
 OR json_type(NEW.request_json,'$.assignmentPayloadDigest') IS NOT 'text'
 OR NOT EXISTS (SELECT 1 FROM validation_jobs j WHERE j.id=NEW.subject_id AND j.state='accepted'
  AND j.id=json_extract(NEW.request_json,'$.assignment.jobID')
  AND j.round_id=json_extract(NEW.request_json,'$.assignment.roundID')
  AND j.assignment_id=json_extract(NEW.request_json,'$.assignment.assignmentID')
  AND j.assignment_epoch=json_extract(NEW.request_json,'$.assignment.assignmentEpoch')
  AND j.report_json=json_extract(NEW.request_json,'$.conclusion'))
)
BEGIN SELECT RAISE(ABORT, 'invalid validation report source'); END;
` },
  { version: 50, name: "fixed-action-request-values", sql: `
CREATE TABLE action_request_staging (
 value_id TEXT PRIMARY KEY REFERENCES value_manifests(id) ON DELETE CASCADE,
 frame_id TEXT NOT NULL REFERENCES frames(id), round_id TEXT NOT NULL REFERENCES prepare_rounds(id),
 generation INTEGER NOT NULL CHECK(generation>=0), assignment_id TEXT NOT NULL, action_key TEXT NOT NULL
) STRICT;
CREATE TRIGGER action_request_staging_origin BEFORE INSERT ON action_request_staging
WHEN NOT EXISTS (SELECT 1 FROM frames f JOIN dispatch_preparations p ON p.frame_id=f.id
 JOIN prepare_rounds q ON q.id=p.round_id JOIN finalize_jobs j ON j.id=f.id
 WHERE f.id=NEW.frame_id AND f.state='open' AND p.state='ready' AND q.state='pending'
 AND p.round_id=NEW.round_id AND p.generation=NEW.generation AND j.state='accepted' AND j.assignment_id=NEW.assignment_id)
BEGIN SELECT RAISE(ABORT, 'invalid Action request staging owner'); END;
CREATE TRIGGER action_request_staging_immutable BEFORE UPDATE ON action_request_staging
WHEN NEW.value_id IS NOT OLD.value_id OR NEW.frame_id IS NOT OLD.frame_id OR NEW.round_id IS NOT OLD.round_id
 OR NEW.generation IS NOT OLD.generation OR NEW.assignment_id IS NOT OLD.assignment_id OR NEW.action_key IS NOT OLD.action_key
BEGIN SELECT RAISE(ABORT, 'immutable Action request staging owner'); END;
ALTER TABLE actions ADD COLUMN request_value_id TEXT GENERATED ALWAYS AS (json_extract(request_json,'$.valueSource.value.id')) VIRTUAL
 REFERENCES value_manifests(id) DEFERRABLE INITIALLY DEFERRED;
CREATE TRIGGER action_fixed_request_source BEFORE INSERT ON actions
WHEN json_type(NEW.request_json,'$.valueSource') IS NOT NULL AND (
 json_extract(NEW.request_json,'$.valueSource.encoding') IS NOT 'action-input-config-v1'
 OR json_type(NEW.request_json,'$.input') IS NOT 'null' OR json_type(NEW.request_json,'$.config') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM action_request_staging s JOIN frames f ON f.id=s.frame_id
 JOIN value_manifests m ON m.id=s.value_id WHERE s.value_id=NEW.request_value_id AND f.run_id=NEW.run_id
 AND s.action_key=NEW.action_key AND s.frame_id=json_extract(NEW.request_json,'$.valueSource.frameID')
 AND s.round_id=json_extract(NEW.request_json,'$.valueSource.roundID') AND s.generation=json_extract(NEW.request_json,'$.valueSource.generation')
 AND m.state='published' AND m.owner=json_extract(NEW.request_json,'$.valueSource.value.owner')
 AND m.encoding=json_extract(NEW.request_json,'$.valueSource.value.encoding')
 AND m.total_bytes=json_extract(NEW.request_json,'$.valueSource.value.length') AND m.digest=json_extract(NEW.request_json,'$.valueSource.value.digest'))
)
BEGIN SELECT RAISE(ABORT, 'invalid fixed Action request'); END;
CREATE TRIGGER action_fixed_request_receipt BEFORE INSERT ON history
WHEN NEW.type='core.suspend' AND NEW.operation_id IS NOT NULL AND json_type(NEW.request_json,'$.actionRequestValues') IS NOT NULL AND (
 json_extract(NEW.request_json,'$.encoding') IS NOT 'core-dispatch-ref-v1'
 OR json_type(NEW.request_json,'$.actionRequestValues') IS NOT 'array'
 OR EXISTS (SELECT 1 FROM json_each(NEW.request_json,'$.actionRequestValues') v WHERE
  json_extract(v.value,'$.source.frameID') IS NOT json_extract(NEW.request_json,'$.frameID')
  OR json_extract(v.value,'$.source.roundID') IS NOT json_extract(NEW.request_json,'$.roundID')
  OR json_extract(v.value,'$.source.generation') IS NOT json_extract(NEW.request_json,'$.generation')
  OR NOT EXISTS (SELECT 1 FROM actions a WHERE a.run_id=NEW.subject_id AND a.action_key=json_extract(v.value,'$.key')
   AND json_extract(a.request_json,'$.valueSource')=json_extract(v.value,'$.source')))
 OR EXISTS (SELECT 1 FROM actions a WHERE a.run_id=NEW.subject_id
  AND json_extract(a.request_json,'$.valueSource.frameID')=json_extract(NEW.request_json,'$.frameID')
  AND (SELECT count(*) FROM json_each(NEW.request_json,'$.actionRequestValues') v
    WHERE json_extract(v.value,'$.key')=a.action_key AND json_extract(v.value,'$.source')=json_extract(a.request_json,'$.valueSource'))<>1)
)
BEGIN SELECT RAISE(ABORT, 'invalid Action request publication receipt'); END;
CREATE TRIGGER action_claim_source BEFORE INSERT ON history
WHEN json_extract(NEW.result_json,'$.assignment.encoding')='action-assignment-v1' AND (
 NEW.type<>'validation.claimed' OR NEW.operation_id IS NULL
 OR NOT EXISTS (SELECT 1 FROM actions a JOIN business_attempts t ON t.action_id=a.id
 WHERE a.id=json_extract(NEW.result_json,'$.assignment.assignment.actionID')
 AND a.run_id=json_extract(NEW.result_json,'$.assignment.assignment.runID')
 AND t.id=json_extract(NEW.result_json,'$.assignment.assignment.attemptID')
 AND a.request_json=json_extract(NEW.result_json,'$.assignment.assignment.request') AND a.request_value_id IS NOT NULL)
)
BEGIN SELECT RAISE(ABORT, 'invalid Action claim source'); END;
` },
  { version: 51, name: "child-run-input-source", sql: `
ALTER TABLE runs ADD COLUMN input_action_id TEXT GENERATED ALWAYS AS (json_extract(snapshot_json,'$.inputSource.actionID')) VIRTUAL REFERENCES actions(id) DEFERRABLE INITIALLY DEFERRED;
CREATE TRIGGER child_run_input_insert BEFORE INSERT ON runs
WHEN json_type(NEW.snapshot_json,'$.inputSource') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.inputSource.encoding') IS NOT 'parent-action-input-v1'
 OR NEW.input_action_id IS NOT json_extract(NEW.snapshot_json,'$.parentActionID')
 OR json_type(NEW.snapshot_json,'$.inputSource.digest') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.config') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM actions a WHERE a.id=NEW.input_action_id AND json_extract(a.request_json,'$.kind')='agent'
  AND json_extract(a.request_json,'$.target')=json_extract(NEW.snapshot_json,'$.agent')
  AND (a.request_value_id IS NULL OR json_extract(a.request_json,'$.valueSource.value.digest')=json_extract(NEW.snapshot_json,'$.inputSource.digest')))
)
BEGIN SELECT RAISE(ABORT, 'invalid child Run input source'); END;
CREATE TRIGGER child_run_input_update BEFORE UPDATE OF snapshot_json ON runs
WHEN (json_type(OLD.snapshot_json,'$.inputSource') IS NOT NULL AND json_extract(OLD.snapshot_json,'$.inputSource') IS NOT json_extract(NEW.snapshot_json,'$.inputSource'))
 OR (json_type(NEW.snapshot_json,'$.inputSource') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.inputSource.encoding') IS NOT 'parent-action-input-v1'
 OR NEW.input_action_id IS NOT NEW.parent_action_id
 OR json_type(NEW.snapshot_json,'$.inputSource.digest') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.config') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM actions a WHERE a.id=NEW.input_action_id AND json_extract(a.request_json,'$.kind')='agent'
  AND json_extract(a.request_json,'$.target')=json_extract(NEW.snapshot_json,'$.agent')
  AND (a.request_value_id IS NULL OR json_extract(a.request_json,'$.valueSource.value.digest')=json_extract(NEW.snapshot_json,'$.inputSource.digest')))))
BEGIN SELECT RAISE(ABORT, 'invalid child Run input update'); END;
CREATE TRIGGER child_frame_input_source BEFORE INSERT ON frames
WHEN json_type(NEW.input_json,'$.run.inputSource') IS NOT NULL AND (
 json_extract(NEW.input_json,'$.frameID') IS NOT NEW.id OR json_extract(NEW.input_json,'$.run.id') IS NOT NEW.run_id
 OR NOT EXISTS (SELECT 1 FROM runs r WHERE r.id=NEW.run_id
  AND json_extract(r.snapshot_json,'$.inputSource')=json_extract(NEW.input_json,'$.run.inputSource'))
 OR json_type(NEW.input_json,'$.run.input') IS NOT 'null' OR json_type(NEW.input_json,'$.run.config') IS NOT 'null'
)
BEGIN SELECT RAISE(ABORT, 'invalid child frame input'); END;
CREATE TRIGGER child_acceptance_input_source BEFORE INSERT ON history
WHEN NEW.type='run.accepted' AND json_type(NEW.data_json,'$.inputSource') IS NOT NULL AND (
 json_extract(NEW.data_json,'$.runID') IS NOT NEW.subject_id OR json_type(NEW.data_json,'$.input') IS NOT 'null' OR json_type(NEW.data_json,'$.config') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM runs r JOIN remote_run_links l ON l.child_run_id=r.id
  WHERE r.id=NEW.subject_id AND l.action_id=r.input_action_id AND json_extract(r.snapshot_json,'$.inputSource')=json_extract(NEW.data_json,'$.inputSource'))
)
BEGIN SELECT RAISE(ABORT, 'invalid child acceptance source'); END;
CREATE TRIGGER driver_child_assignment_insert BEFORE INSERT ON driver_jobs
WHEN json_extract(NEW.assignment_json,'$.encoding')='child-frame-assignment-v1' AND (
 NEW.assignment_encoding<>'inline' OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
 OR json_extract(NEW.assignment_json,'$.assignment.driver') IS NOT 1
 OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
 OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
 OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='inline' AND json_type(f.input_json,'$.run.inputSource')='object')
)
BEGIN SELECT RAISE(ABORT, 'invalid child frame assignment'); END;
CREATE TRIGGER driver_child_assignment_update BEFORE UPDATE ON driver_jobs
WHEN json_extract(NEW.assignment_json,'$.encoding')='child-frame-assignment-v1' AND (
 NEW.assignment_encoding<>'inline' OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
 OR json_extract(NEW.assignment_json,'$.assignment.driver') IS NOT 1
 OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
 OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
 OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='inline' AND json_type(f.input_json,'$.run.inputSource')='object')
)
BEGIN SELECT RAISE(ABORT, 'invalid child frame assignment'); END;
CREATE TRIGGER finalize_child_assignment_insert BEFORE INSERT ON finalize_jobs
WHEN json_extract(NEW.assignment_json,'$.encoding')='child-frame-assignment-v1' AND (
 NEW.assignment_encoding<>'inline' OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
 OR json_extract(NEW.assignment_json,'$.assignment.finalize') IS NOT 1
 OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
 OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
 OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='inline' AND json_type(f.input_json,'$.run.inputSource')='object')
)
BEGIN SELECT RAISE(ABORT, 'invalid child frame assignment'); END;
CREATE TRIGGER finalize_child_assignment_update BEFORE UPDATE ON finalize_jobs
WHEN json_extract(NEW.assignment_json,'$.encoding')='child-frame-assignment-v1' AND (
 NEW.assignment_encoding<>'inline' OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
 OR json_extract(NEW.assignment_json,'$.assignment.finalize') IS NOT 1
 OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
 OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
 OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='inline' AND json_type(f.input_json,'$.run.inputSource')='object')
)
BEGIN SELECT RAISE(ABORT, 'invalid child frame assignment'); END;
CREATE TRIGGER child_frame_claim_source BEFORE INSERT ON history
WHEN json_extract(NEW.result_json,'$.assignment.encoding')='child-frame-assignment-v1' AND (
 NEW.type<>'validation.claimed' OR NEW.operation_id IS NULL OR NOT EXISTS (
 SELECT 1 FROM driver_jobs j WHERE j.assignment_json=json_extract(NEW.result_json,'$.assignment') AND j.id=json_extract(NEW.result_json,'$.assignment.frameID')
 UNION ALL SELECT 1 FROM finalize_jobs j WHERE j.assignment_json=json_extract(NEW.result_json,'$.assignment') AND j.id=json_extract(NEW.result_json,'$.assignment.frameID'))
)
BEGIN SELECT RAISE(ABORT, 'invalid child claim receipt'); END;
` },
{ version: 52, name: "root-submit-input-source", sql: `
CREATE TABLE submit_input_staging (
 value_id TEXT PRIMARY KEY REFERENCES value_manifests(id) ON DELETE CASCADE,
 coordinator_epoch INTEGER NOT NULL CHECK(coordinator_epoch>0),
 state TEXT NOT NULL CHECK(state IN ('active','abandoned'))
) STRICT;
CREATE TRIGGER submit_input_staging_owner BEFORE UPDATE ON submit_input_staging
WHEN NEW.value_id<>OLD.value_id OR NEW.coordinator_epoch<>OLD.coordinator_epoch OR (OLD.state='abandoned' AND NEW.state<>'abandoned')
BEGIN SELECT RAISE(ABORT, 'immutable submit staging owner'); END;
CREATE TABLE root_run_inputs (
 run_id TEXT PRIMARY KEY REFERENCES runs(id), principal_id TEXT NOT NULL, request_key TEXT NOT NULL,
 FOREIGN KEY(principal_id,request_key) REFERENCES commands(principal_id,request_key)
) STRICT;
CREATE TRIGGER root_run_input_binding BEFORE INSERT ON root_run_inputs
WHEN NOT EXISTS (SELECT 1 FROM runs r JOIN commands c ON c.principal_id=NEW.principal_id AND c.request_key=NEW.request_key
 WHERE r.id=NEW.run_id AND r.principal_id=NEW.principal_id AND c.operation='submit' AND c.status='applied'
 AND json_extract(c.result_json,'$.runID')=r.id
 AND json_extract(r.snapshot_json,'$.inputSource.principalID')=NEW.principal_id
 AND json_extract(r.snapshot_json,'$.inputSource.requestKey')=NEW.request_key)
BEGIN SELECT RAISE(ABORT, 'invalid root input binding'); END;
CREATE TRIGGER root_run_inputs_no_update BEFORE UPDATE ON root_run_inputs BEGIN SELECT RAISE(ABORT, 'immutable root input binding'); END;
CREATE TRIGGER root_run_inputs_no_delete BEFORE DELETE ON root_run_inputs BEGIN SELECT RAISE(ABORT, 'retained root input binding'); END;
DROP TRIGGER child_run_input_insert;
DROP TRIGGER child_run_input_update;
CREATE TRIGGER child_run_input_insert BEFORE INSERT ON runs
WHEN json_type(NEW.snapshot_json,'$.inputSource') IS NOT NULL AND json_extract(NEW.snapshot_json,'$.inputSource.encoding') IS NOT 'root-submit-input-v1' AND (
 json_extract(NEW.snapshot_json,'$.inputSource.encoding') IS NOT 'parent-action-input-v1'
 OR NEW.input_action_id IS NOT json_extract(NEW.snapshot_json,'$.parentActionID')
 OR json_type(NEW.snapshot_json,'$.inputSource.digest') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.config') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM actions a WHERE a.id=NEW.input_action_id AND json_extract(a.request_json,'$.kind')='agent'
  AND json_extract(a.request_json,'$.target')=json_extract(NEW.snapshot_json,'$.agent')
  AND (a.request_value_id IS NULL OR json_extract(a.request_json,'$.valueSource.value.digest')=json_extract(NEW.snapshot_json,'$.inputSource.digest')))
)
BEGIN SELECT RAISE(ABORT, 'invalid child Run input source'); END;
CREATE TRIGGER child_run_input_update BEFORE UPDATE OF snapshot_json ON runs
WHEN (json_type(OLD.snapshot_json,'$.inputSource') IS NOT NULL AND json_extract(OLD.snapshot_json,'$.inputSource') IS NOT json_extract(NEW.snapshot_json,'$.inputSource'))
 OR (json_type(NEW.snapshot_json,'$.inputSource') IS NOT NULL AND json_extract(NEW.snapshot_json,'$.inputSource.encoding') IS NOT 'root-submit-input-v1' AND (
 json_extract(NEW.snapshot_json,'$.inputSource.encoding') IS NOT 'parent-action-input-v1'
 OR NEW.input_action_id IS NOT NEW.parent_action_id
 OR json_type(NEW.snapshot_json,'$.inputSource.digest') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.config') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM actions a WHERE a.id=NEW.input_action_id AND json_extract(a.request_json,'$.kind')='agent'
  AND json_extract(a.request_json,'$.target')=json_extract(NEW.snapshot_json,'$.agent')
  AND (a.request_value_id IS NULL OR json_extract(a.request_json,'$.valueSource.value.digest')=json_extract(NEW.snapshot_json,'$.inputSource.digest')))))
BEGIN SELECT RAISE(ABORT, 'invalid child Run input update'); END;
CREATE TRIGGER root_run_source_insert BEFORE INSERT ON runs
WHEN json_extract(NEW.snapshot_json,'$.inputSource.encoding')='root-submit-input-v1' AND (
 json_type(NEW.snapshot_json,'$.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.config') IS NOT 'null'
 OR json_type(NEW.snapshot_json,'$.parentActionID') IS NOT 'null' OR json_extract(NEW.snapshot_json,'$.rootID') IS NOT NEW.id
 OR NOT EXISTS (SELECT 1 FROM commands c JOIN value_manifests m ON m.id=json_extract(c.request_json,'$.inputSource.value.id')
 WHERE c.principal_id=NEW.principal_id AND c.request_key=json_extract(NEW.snapshot_json,'$.inputSource.requestKey')
 AND c.principal_id=json_extract(NEW.snapshot_json,'$.inputSource.principalID') AND c.operation='submit' AND c.status='applied'
 AND json_extract(c.result_json,'$.runID')=NEW.id AND json_extract(c.result_json,'$.agent')=json_extract(NEW.snapshot_json,'$.agent')
 AND json_extract(c.request_json,'$.inputSource.encoding')='submit-input-v1'
 AND m.state='published' AND m.digest=json_extract(NEW.snapshot_json,'$.inputSource.digest'))
)
BEGIN SELECT RAISE(ABORT, 'invalid root Run input source'); END;
CREATE TRIGGER root_run_source_update BEFORE UPDATE OF snapshot_json ON runs
WHEN json_extract(NEW.snapshot_json,'$.inputSource.encoding')='root-submit-input-v1' AND (
 json_type(NEW.snapshot_json,'$.input') IS NOT 'null' OR json_type(NEW.snapshot_json,'$.config') IS NOT 'null'
 OR json_type(NEW.snapshot_json,'$.parentActionID') IS NOT 'null' OR json_extract(NEW.snapshot_json,'$.rootID') IS NOT NEW.id
 OR NOT EXISTS (SELECT 1 FROM commands c JOIN value_manifests m ON m.id=json_extract(c.request_json,'$.inputSource.value.id')
 WHERE c.principal_id=NEW.principal_id AND c.request_key=json_extract(NEW.snapshot_json,'$.inputSource.requestKey')
 AND c.principal_id=json_extract(NEW.snapshot_json,'$.inputSource.principalID') AND c.operation='submit' AND c.status='applied'
 AND json_extract(c.result_json,'$.runID')=NEW.id AND json_extract(c.result_json,'$.agent')=json_extract(NEW.snapshot_json,'$.agent')
 AND json_extract(c.request_json,'$.inputSource.encoding')='submit-input-v1'
 AND m.state='published' AND m.digest=json_extract(NEW.snapshot_json,'$.inputSource.digest'))
)
BEGIN SELECT RAISE(ABORT, 'invalid root Run input source'); END;
` },
{ version: 53, name: "continuation-and-checkpoint-sources", sql: continuationMigrationSQL + checkpointValueMigrationSQL },
{ version: 54, name: "durable-command-values", sql: commandValueMigration },
{ version: 55, name: "fixed-node-policy-source", sql: policyValueMigration },
{ version: 56, name: "validation-conclusion-source", sql: validationValueMigrationSQL },
] as const;

export const schemaVersion = migrations.length;

/** Synchronous, startup-only migrations; targetVersion is for upgrade fixtures. */
export function migrate(db: DatabaseSync, targetVersion: number = schemaVersion): void {
  if (!Number.isInteger(targetVersion) || targetVersion < 1 || targetVersion > schemaVersion) {
    throw new Error("Unsupported target schema version");
  }
  db.exec("BEGIN IMMEDIATE");
  try {
    const exists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'").get();
    if (!exists) {
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
      if (tables.length) throw new Error("Not a formal Harness database; prototype schemas cannot be upgraded");
      db.exec(`CREATE TABLE schema_migrations (
        version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at INTEGER NOT NULL
      ) STRICT`);
    }
    const applied = db.prepare("SELECT version, name, checksum FROM schema_migrations ORDER BY version").all();
    const userVersion = db.prepare("PRAGMA user_version").get()!.user_version;
    if (userVersion !== applied.length || applied.length > targetVersion) throw new Error("Incompatible schema version");
    for (const [i, row] of applied.entries()) {
      const migration = migrations[i];
      if (!migration || row.version !== migration.version || row.name !== migration.name || row.checksum !== checksum(migration.sql)) {
        throw new Error("Schema migration history mismatch");
      }
    }
    for (const migration of migrations.slice(applied.length, targetVersion)) {
      db.exec(migration.sql);
      if (migration.version === 4) {
        // One-time upgrade, not a normal commit/renewal path. Preserve original receipts.
        for (const row of db.prepare("SELECT sequence,type,subject_id,request_json FROM history").all()) {
          const legacy = parseJson(row.request_json as string) as { [key: string]: Json };
          if (!legacy || typeof legacy !== "object" || Array.isArray(legacy) || !("request" in legacy)) throw new Error("Invalid legacy operation identity");
          const request = canonicalJson(legacy.request);
          const hash = checksum(canonicalJson({ kind: row.type as string, subjectID: row.subject_id as string, request: legacy.request }));
          db.prepare("UPDATE history SET request_json=?,request_hash=? WHERE sequence=?").run(request, hash, row.sequence!);
        }
        db.exec("CREATE TRIGGER history_no_update BEFORE UPDATE ON history BEGIN SELECT RAISE(ABORT, 'append-only history'); END");
      }
      if (migration.version === 1) {
        db.prepare("INSERT INTO domain VALUES (1, ?, 'harness/1', ?)").run(randomUUID(), randomBytes(32).toString("hex"));
      }
      db.prepare("INSERT INTO schema_migrations VALUES (?, ?, ?, ?)").run(migration.version, migration.name, checksum(migration.sql), Date.now());
      db.exec(`PRAGMA user_version = ${migration.version}`);
    }
    // Metadata alone cannot make a damaged/missing schema safe to use.
    const objects = new Map(db.prepare("SELECT name, type FROM sqlite_master").all().map(row => [row.name, row.type]));
    for (const migration of migrations.slice(0, targetVersion)) {
      for (const match of migration.sql.matchAll(/\bCREATE\s+(?:UNIQUE\s+)?(TABLE|INDEX|TRIGGER)\s+(\w+)/g)) {
        if (objects.get(match[2]!) !== match[1]!.toLowerCase()) throw new Error("Incomplete formal Harness schema");
      }
    }
    if (db.prepare("SELECT count(*) n FROM domain WHERE singleton=1 AND protocol_version='harness/1'").get()!.n !== 1) throw new Error("Invalid formal Harness database");
    db.exec("COMMIT");
  } catch (error) {
    if (db.isTransaction) db.exec("ROLLBACK");
    throw error;
  }
}

function checksum(sql: string): string {
  return createHash("sha256").update(sql).digest("hex");
}
