/** New private source forms keep the existing frame-assignment descriptor.
 * Only its eligibility guard expands; old migration SQL stays immutable. */
export const continuationMigrationSQL = `
CREATE TRIGGER run_continuation_source_insert BEFORE INSERT ON runs
WHEN json_type(NEW.snapshot_json,'$.continuationSource') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.continuationSource.encoding') IS NOT 'driver-continuation-v1'
 OR json_extract(NEW.snapshot_json,'$.continuationSource.runID') IS NOT NEW.id
 OR json_type(NEW.snapshot_json,'$.continuationSource.operationID') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.continuationSource.digest') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.continuation.state') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM frames f JOIN frame_candidates c ON c.frame_id=f.id
 WHERE f.id=json_extract(NEW.snapshot_json,'$.continuationSource.frameID') AND f.run_id=NEW.id)
)
BEGIN SELECT RAISE(ABORT, 'invalid Run continuation source'); END;
CREATE TRIGGER run_continuation_source_update BEFORE UPDATE OF snapshot_json ON runs
WHEN json_type(NEW.snapshot_json,'$.continuationSource') IS NOT NULL AND (
 json_extract(NEW.snapshot_json,'$.continuationSource.encoding') IS NOT 'driver-continuation-v1'
 OR json_extract(NEW.snapshot_json,'$.continuationSource.runID') IS NOT NEW.id
 OR json_type(NEW.snapshot_json,'$.continuationSource.operationID') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.continuationSource.digest') IS NOT 'text'
 OR json_type(NEW.snapshot_json,'$.continuation.state') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM frames f JOIN frame_candidates c ON c.frame_id=f.id
 WHERE f.id=json_extract(NEW.snapshot_json,'$.continuationSource.frameID') AND f.run_id=NEW.id)
)
BEGIN SELECT RAISE(ABORT, 'invalid Run continuation source'); END;
` + ["driver", "finalize"].flatMap(kind => ["insert", "update"].map(event => `
DROP TRIGGER ${kind}_child_assignment_${event};
CREATE TRIGGER ${kind}_child_assignment_${event} BEFORE ${event.toUpperCase()} ON ${kind}_jobs
WHEN json_extract(NEW.assignment_json,'$.encoding')='child-frame-assignment-v1' AND (
 NEW.assignment_encoding<>'inline' OR json_extract(NEW.assignment_json,'$.frameID') IS NOT NEW.id
 OR json_extract(NEW.assignment_json,'$.assignment.${kind}') IS NOT 1
 OR json_extract(NEW.assignment_json,'$.assignment.jobID') IS NOT NEW.id OR json_extract(NEW.assignment_json,'$.assignment.runID') IS NOT NEW.run_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentID') IS NOT NEW.assignment_id
 OR json_extract(NEW.assignment_json,'$.assignment.assignmentEpoch') IS NOT NEW.assignment_epoch
 OR json_extract(NEW.assignment_json,'$.assignment.incarnation') IS NOT NEW.node_incarnation
 OR json_type(NEW.assignment_json,'$.assignment.input') IS NOT 'null'
 OR NOT EXISTS (SELECT 1 FROM frames f WHERE f.id=NEW.id AND f.run_id=NEW.run_id AND f.input_encoding='inline'
  AND (json_type(f.input_json,'$.run.inputSource')='object' OR json_type(f.input_json,'$.continuationSource')='object'))
)
BEGIN SELECT RAISE(ABORT, 'invalid referenced frame assignment'); END;
`)).join("");
