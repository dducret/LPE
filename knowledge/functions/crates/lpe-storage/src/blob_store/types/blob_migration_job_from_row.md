---
type: Rust Function
title: blob_migration_job_from_row
resource: crates/lpe-storage/src/blob_store/types.rs#L151-L165
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/record_blob_migration_failure
---

# Signature

`pub(super) fn blob_migration_job_from_row(row: sqlx::postgres::PgRow) -> Result<BlobMigrationJob>`

# Called by

- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [copy_and_verify_one_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)
- [switch_verified_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/switch_verified_blob_migration_job.md)
- [record_blob_migration_failure](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/record_blob_migration_failure.md)