---
type: Rust Method
title: upsert_public_folder_replica
resource: crates/lpe-storage/src/public_folders.rs#L1023-L1091
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_share
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/public_folders/types/map_public_folder_replica
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/public_folders/put_public_folder_replica
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_replica_path
---

# Signature

`pub async fn upsert_public_folder_replica( &self, input: PublicFolderReplicaInput, audit: AuditEntryInput, ) -> Result<PublicFolderReplica>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_share](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_share.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [record_public_folder_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [map_public_folder_replica](../../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder_replica.md)

# Called by

- [put_public_folder_replica](../../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/put_public_folder_replica.md)
- [exercise_public_folder_replica_path](../../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_replica_path.md)