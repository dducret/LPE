---
type: Rust Method
title: ensure_copying_target_placement
resource: crates/lpe-storage/src/blob_store.rs#L743-L823
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
---

# Signature

`async fn ensure_copying_target_placement( &self, pool: &PgPool, job: &BlobMigrationJob, blob: &StoredBlobBytes, ) -> Result<MigrationTargetPlacement>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [select_storage_backend](../../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)

# Called by

- [copy_and_verify_one_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)