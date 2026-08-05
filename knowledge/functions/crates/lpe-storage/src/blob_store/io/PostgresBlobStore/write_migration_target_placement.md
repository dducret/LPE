---
type: Rust Method
title: write_migration_target_placement
resource: crates/lpe-storage/src/blob_store/io.rs#L184-L263
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
---

# Signature

`pub(super) async fn write_migration_target_placement( &self, pool: &PgPool, job: &BlobMigrationJob, target: &MigrationTargetPlacement, blob: &StoredBlobBytes, ) -> Result<()>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [s3_put_object](../../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)

# Called by

- [copy_and_verify_one_blob_migration_job](../../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)