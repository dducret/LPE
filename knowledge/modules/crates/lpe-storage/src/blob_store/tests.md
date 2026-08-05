---
type: Rust Module
title: tests
resource: crates/lpe-storage/src/blob_store/tests.rs#L1-L2801
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/crate-attachmentuploadinput-storage
  - external/serde-json-json-value
  - external/sqlx-postgres-pgpooloptions
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [test_storage](../../../../../functions/crates/lpe-storage/src/blob_store/tests/test_storage.md)
- [insert_tenant_domain](../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_tenant_domain.md)
- [s3_test_config](../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_test_config.md)
- [s3_placeholder_config](../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_placeholder_config.md)
- [insert_s3_storage_pool](../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_s3_storage_pool.md)
- [configure_s3_platform_pool](../../../../../functions/crates/lpe-storage/src/blob_store/tests/configure_s3_platform_pool.md)
- [insert_account_mailbox](../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_account_mailbox.md)
- [insert_logical_message_with_attachment](../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment.md)
- [logical_quota_snapshot](../../../../../functions/crates/lpe-storage/src/blob_store/tests/logical_quota_snapshot.md)
- [expire_retiring_placement](../../../../../functions/crates/lpe-storage/src/blob_store/tests/expire_retiring_placement.md)
- [mark_active_replacement_failed](../../../../../functions/crates/lpe-storage/src/blob_store/tests/mark_active_replacement_failed.md)
- [cleanup_blockers](../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_blockers.md)
- [placement_status_by_id](../../../../../functions/crates/lpe-storage/src/blob_store/tests/placement_status_by_id.md)
- [active_placement_id](../../../../../functions/crates/lpe-storage/src/blob_store/tests/active_placement_id.md)
- [assert_active_blob_read](../../../../../functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read.md)
- [active_placement_count](../../../../../functions/crates/lpe-storage/src/blob_store/tests/active_placement_count.md)
- [placement_count_for_pool](../../../../../functions/crates/lpe-storage/src/blob_store/tests/placement_count_for_pool.md)
- [active_storage_pool_id](../../../../../functions/crates/lpe-storage/src/blob_store/tests/active_storage_pool_id.md)
- [database_blob_bytes_len](../../../../../functions/crates/lpe-storage/src/blob_store/tests/database_blob_bytes_len.md)
- [insert_secondary_storage_pool](../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_secondary_storage_pool.md)
- [put_test_blob](../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [create_verified_migration_job](../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_verified_migration_job.md)
- [create_blob_migration_job_accepts_attachment_and_mime_part_blobs](../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_attachment_and_mime_part_blobs.md)
- [create_blob_migration_job_accepts_s3_compatible_target_pool](../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_accepts_s3_compatible_target_pool.md)
- [duplicate_blob_migration_job_create_returns_existing_open_job](../../../../../functions/crates/lpe-storage/src/blob_store/tests/duplicate_blob_migration_job_create_returns_existing_open_job.md)
- [create_blob_migration_job_rejects_raw_message_kind](../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_raw_message_kind.md)
- [create_blob_migration_job_rejects_missing_active_source_placement](../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_missing_active_source_placement.md)
- [create_blob_migration_job_rejects_same_source_and_target_pool](../../../../../functions/crates/lpe-storage/src/blob_store/tests/create_blob_migration_job_rejects_same_source_and_target_pool.md)
- [pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order](../../../../../functions/crates/lpe-storage/src/blob_store/tests/pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order.md)
- [copy_verify_worker_reuses_target_placement_across_repeated_execution](../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_reuses_target_placement_across_repeated_execution.md)
- [copy_verify_worker_leaves_active_source_read_path_unchanged](../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged.md)
- [copy_verify_worker_records_retryable_failure_without_switching_source](../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_records_retryable_failure_without_switching_source.md)
- [switch_verified_migration_job_leaves_one_active_target_placement](../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_verified_migration_job_leaves_one_active_target_placement.md)
- [repeated_switch_verified_migration_job_is_idempotent](../../../../../functions/crates/lpe-storage/src/blob_store/tests/repeated_switch_verified_migration_job_is_idempotent.md)
- [switch_preserves_reads_stats_and_verification_across_phases](../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases.md)
- [switch_writes_rollback_window_to_retiring_source_placement](../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_writes_rollback_window_to_retiring_source_placement.md)
- [logical_quota_is_stable_across_deduplicated_blob_migration](../../../../../functions/crates/lpe-storage/src/blob_store/tests/logical_quota_is_stable_across_deduplicated_blob_migration.md)
- [retiring_placement_cleanup_is_blocked_by_rollback_window](../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_rollback_window.md)
- [retiring_placement_cleanup_is_blocked_when_live_references_need_it](../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it.md)
- [retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold](../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold.md)
- [cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads](../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads.md)
- [cleanup_worker_refuses_the_only_active_placement](../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement.md)
- [cleanup_worker_repeated_execution_is_idempotent](../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent.md)
- [cleanup_worker_records_retryable_failure_without_breaking_active_reads](../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads.md)
- [cleanup_worker_claims_due_old_placements_deterministically](../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_claims_due_old_placements_deterministically.md)
- [switch_ignores_unverified_migration_jobs](../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_ignores_unverified_migration_jobs.md)
- [durable_blob_store_writes_reads_stats_and_verifies](../../../../../functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies.md)
- [s3_compatible_backend_put_read_stat_and_verify_round_trip](../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip.md)
- [s3_compatible_migration_paths_copy_verify_and_switch](../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_migration_paths_copy_verify_and_switch.md)
- [attachment_content_fetch_reads_through_blob_store_boundary](../../../../../functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary.md)

# Imports

- `super::*`
- `crate::{AttachmentUploadInput, Storage}`
- `serde_json::{json, Value}`
- `sqlx::postgres::PgPoolOptions`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)