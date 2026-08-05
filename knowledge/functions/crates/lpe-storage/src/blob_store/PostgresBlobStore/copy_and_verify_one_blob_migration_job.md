---
type: Rust Method
title: copy_and_verify_one_blob_migration_job
resource: crates/lpe-storage/src/blob_store.rs#L406-L519
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/claim_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/types/durable_blob_kind_from_str
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_migration_source_placement
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/record_blob_migration_failure
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_placement_bytes
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_copying_target_placement
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/write_migration_target_placement
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/types/blob_migration_job_from_row
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_reuses_target_placement_across_repeated_execution
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_records_retryable_failure_without_switching_source
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch
  - functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source
---

# Signature

`pub(crate) async fn copy_and_verify_one_blob_migration_job( &self, pool: &PgPool, ) -> Result<Option<BlobMigrationJob>>`

# Calls

- [claim_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/claim_blob_migration_job.md)
- [durable_blob_kind_from_str](../../../../../../functions/crates/lpe-storage/src/blob_store/types/durable_blob_kind_from_str.md)
- [load_migration_source_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_migration_source_placement.md)
- [record_blob_migration_failure](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/record_blob_migration_failure.md)
- [read_placement_bytes](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_placement_bytes.md)
- [ensure_copying_target_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_copying_target_placement.md)
- [write_migration_target_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/write_migration_target_placement.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [blob_migration_job_from_row](../../../../../../functions/crates/lpe-storage/src/blob_store/types/blob_migration_job_from_row.md)

# Called by

- [create_verified_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job.md)
- [copy_verify_worker_reuses_target_placement_across_repeated_execution](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_reuses_target_placement_across_repeated_execution.md)
- [copy_verify_worker_leaves_active_source_read_path_unchanged](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged.md)
- [copy_verify_worker_records_retryable_failure_without_switching_source](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_records_retryable_failure_without_switching_source.md)
- [s3_compatible_migration_paths_copy_verify_and_switch](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch.md)
- [migrate_attachment_and_cleanup_source](../../../../../../functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source.md)