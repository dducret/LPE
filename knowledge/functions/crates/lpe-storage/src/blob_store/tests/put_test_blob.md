---
type: Rust Function
title: put_test_blob
resource: crates/lpe-storage/src/blob_store/tests.rs#L472-L500
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_attachment_and_mime_part_blobs
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_s3_compatible_target_pool
  - functions/crates/lpe-storage/src/blob_store/tests/duplicate_blob_migration_job_create_returns_existing_open_job
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_missing_active_source_placement
  - functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_same_source_and_target_pool
  - functions/crates/lpe-storage/src/blob_store/tests/pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_reuses_target_placement_across_repeated_execution
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_records_retryable_failure_without_switching_source
  - functions/crates/lpe-storage/src/blob_store/tests/switch_verified_migration_job_leaves_one_active_target_placement
  - functions/crates/lpe-storage/src/blob_store/tests/repeated_switch_verified_migration_job_is_idempotent
  - functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases
  - functions/crates/lpe-storage/src/blob_store/tests/switch_writes_rollback_window_to_retiring_source_placement
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_rollback_window
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_claims_due_old_placements_deterministically
  - functions/crates/lpe-storage/src/blob_store/tests/switch_ignores_unverified_migration_jobs
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch
---

# Signature

`async fn put_test_blob( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, kind: DurableBlobKind, bytes: &[u8], ) -> StoredBlobRef`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [put_durable_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx.md)

# Called by

- [create_blob_migration_job_accepts_attachment_and_mime_part_blobs](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_attachment_and_mime_part_blobs.md)
- [create_blob_migration_job_accepts_s3_compatible_target_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_s3_compatible_target_pool.md)
- [duplicate_blob_migration_job_create_returns_existing_open_job](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/duplicate_blob_migration_job_create_returns_existing_open_job.md)
- [create_blob_migration_job_rejects_missing_active_source_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_missing_active_source_placement.md)
- [create_blob_migration_job_rejects_same_source_and_target_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_same_source_and_target_pool.md)
- [pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order.md)
- [copy_verify_worker_reuses_target_placement_across_repeated_execution](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_reuses_target_placement_across_repeated_execution.md)
- [copy_verify_worker_leaves_active_source_read_path_unchanged](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged.md)
- [copy_verify_worker_records_retryable_failure_without_switching_source](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_records_retryable_failure_without_switching_source.md)
- [switch_verified_migration_job_leaves_one_active_target_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_verified_migration_job_leaves_one_active_target_placement.md)
- [repeated_switch_verified_migration_job_is_idempotent](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/repeated_switch_verified_migration_job_is_idempotent.md)
- [switch_preserves_reads_stats_and_verification_across_phases](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases.md)
- [switch_writes_rollback_window_to_retiring_source_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_writes_rollback_window_to_retiring_source_placement.md)
- [retiring_placement_cleanup_is_blocked_by_rollback_window](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_rollback_window.md)
- [cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads.md)
- [cleanup_worker_refuses_the_only_active_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement.md)
- [cleanup_worker_repeated_execution_is_idempotent](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent.md)
- [cleanup_worker_records_retryable_failure_without_breaking_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads.md)
- [cleanup_worker_claims_due_old_placements_deterministically](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_claims_due_old_placements_deterministically.md)
- [switch_ignores_unverified_migration_jobs](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_ignores_unverified_migration_jobs.md)
- [s3_compatible_migration_paths_copy_verify_and_switch](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch.md)