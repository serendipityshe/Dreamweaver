export const policyValueMigration = `
CREATE TABLE node_policy_staging (
 value_id TEXT PRIMARY KEY REFERENCES value_manifests(id) ON DELETE CASCADE,
 coordinator_epoch INTEGER NOT NULL CHECK(coordinator_epoch>0),
 state TEXT NOT NULL CHECK(state IN ('active','abandoned'))
);
CREATE TRIGGER node_policy_source_insert_guard BEFORE INSERT ON node_sessions
WHEN json_type(NEW.configuration_json,'$.policySource') IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM value_manifests m WHERE m.id=json_extract(NEW.configuration_json,'$.policySource.value.id')
 AND m.state='published' AND m.owner=json_extract(NEW.configuration_json,'$.policySource.value.owner')
 AND m.digest=json_extract(NEW.configuration_json,'$.policySource.value.digest')
 AND json_extract(NEW.configuration_json,'$.policySource.incarnation')=NEW.incarnation
 AND json_extract(NEW.configuration_json,'$.policySource.nodeID')=NEW.node_id
 AND json_extract(NEW.configuration_json,'$.policySource.principalID')=NEW.principal_id)
BEGIN SELECT RAISE(ABORT,'invalid node policy source'); END;
DROP TRIGGER action_claim_source;
CREATE TRIGGER action_claim_source BEFORE INSERT ON history
WHEN json_extract(NEW.result_json,'$.assignment.encoding')='action-assignment-v1' AND (
 NEW.type<>'validation.claimed' OR NEW.operation_id IS NULL OR NOT EXISTS
 (SELECT 1 FROM actions a JOIN business_attempts t ON t.action_id=a.id
 WHERE a.id=json_extract(NEW.result_json,'$.assignment.assignment.actionID')
 AND a.run_id=json_extract(NEW.result_json,'$.assignment.assignment.runID')
 AND t.id=json_extract(NEW.result_json,'$.assignment.assignment.attemptID')
 AND a.request_json=json_extract(NEW.result_json,'$.assignment.assignment.request')
 AND (a.request_value_id IS NOT NULL OR json_type(a.request_json,'$.policySource')='object'
  OR json_type(NEW.result_json,'$.assignment.assignment.checkpoint.valueSource')='object')))
BEGIN SELECT RAISE(ABORT,'invalid Action claim source'); END;
`;
