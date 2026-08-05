---
type: Rust Method
title: create_blob_migration_job
resource: crates/lpe-storage/src/blob_store.rs#L253-L377
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/types/normalize_migration_blob_kind
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/existing_open_migration_job
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  - functions/crates/lpe-storage/src/blob_store/types/blob_migration_job_from_row
  - functions/crates/lpe-storage/src/blob_store/types/is_constraint_error
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_attachment_and_mime_part_blobs
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_s3_compatible_target_pool
  - functions/crates/lpe-storage/src/blob_store/tests/duplicate_blob_migration_job_create_returns_existing_open_job
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_raw_message_kind
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_missing_active_source_placement
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_same_source_and_target_pool
  - functions/crates/lpe-storage/src/blob_store/tests/pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_reuses_target_placement_across_repeated_execution
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_records_retryable_failure_without_switching_source
  - functions/crates/lpe-storage/src/blob_store/tests/switch_ignores_unverified_migration_jobs
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch
  - functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source
---

# Signature

`pub(crate) async fn create_blob_migration_job( &self, pool: &PgPool, tenant_id: &Uuid, domain_id: Uuid, blob_kind: &str, blob_id: Uuid, target_storage_pool_id: Uuid, ) -> Result<BlobMigrationJob>`

# Calls

- [normalize_migration_blob_kind](../../../../../../functions/crates/lpe-storage/src/blob_store/types/normalize_migration_blob_kind.md)
- [existing_open_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/existing_open_migration_job.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [select_storage_backend](../../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)
- [blob_migration_job_from_row](../../../../../../functions/crates/lpe-storage/src/blob_store/types/blob_migration_job_from_row.md)
- [is_constraint_error](../../../../../../functions/crates/lpe-storage/src/blob_store/types/is_constraint_error.md)

# Called by

- [create_verified_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job.md)
- [create_blob_migration_job_accepts_attachment_and_mime_part_blobs](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_attachment_and_mime_part_blobs.md)
- [create_blob_migration_job_accepts_s3_compatible_target_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_s3_compatible_target_pool.md)
- [duplicate_blob_migration_job_create_returns_existing_open_job](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/duplicate_blob_migration_job_create_returns_existing_open_job.md)
- [create_blob_migration_job_rejects_raw_message_kind](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_raw_message_kind.md)
- [create_blob_migration_job_rejects_missing_active_source_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_missing_active_source_placement.md)
- [create_blob_migration_job_rejects_same_source_and_target_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_same_source_and_target_pool.md)
- [pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order.md)
- [copy_verify_worker_reuses_target_placement_across_repeated_execution](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_reuses_target_placement_across_repeated_execution.md)
- [copy_verify_worker_leaves_active_source_read_path_unchanged](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged.md)
- [copy_verify_worker_records_retryable_failure_without_switching_source](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_records_retryable_failure_without_switching_source.md)
- [switch_ignores_unverified_migration_jobs](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_ignores_unverified_migration_jobs.md)
- [s3_compatible_migration_paths_copy_verify_and_switch](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch.md)
- [migrate_attachment_and_cleanup_source](../../../../../../functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source.md)