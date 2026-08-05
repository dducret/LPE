---
type: Rust Method
title: record_blob_migration_failure
resource: crates/lpe-storage/src/blob_store.rs#L826-L852
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/types/blob_migration_job_from_row
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
---

# Signature

`async fn record_blob_migration_failure( &self, pool: &PgPool, job_id: Uuid, error: &str, ) -> Result<BlobMigrationJob>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [blob_migration_job_from_row](../../../../../../functions/crates/lpe-storage/src/blob_store/types/blob_migration_job_from_row.md)

# Called by

- [copy_and_verify_one_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)