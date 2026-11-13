/** Added by the next formal migration; existing migration checksums stay fixed. */
export const checkpointValueMigrationSQL = `
CREATE TABLE checkpoint_value_staging (
 value_id TEXT PRIMARY KEY REFERENCES value_manifests(id) ON DELETE CASCADE,
 coordinator_epoch INTEGER NOT NULL CHECK(coordinator_epoch>0),
 state TEXT NOT NULL CHECK(state IN ('active','abandoned'))
) STRICT;
CREATE TRIGGER checkpoint_value_staging_identity BEFORE UPDATE OF value_id,coordinator_epoch ON checkpoint_value_staging
BEGIN SELECT RAISE(ABORT, 'immutable checkpoint staging owner'); END;
`;
