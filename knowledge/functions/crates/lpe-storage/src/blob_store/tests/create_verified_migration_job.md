---
type: Rust Function
title: create_verified_migration_job
resource: crates/lpe-storage/src/blob_store/tests.rs#L502-L526
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/switch_verified_migration_job_leaves_one_active_target_placement
  - functions/crates/lpe-storage/src/blob_store/tests/repeated_switch_verified_migration_job_is_idempotent
  - functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases
  - functions/crates/lpe-storage/src/blob_store/tests/switch_writes_rollback_window_to_retiring_source_placement
  - functions/crates/lpe-storage/src/blob_store/tests/logical_quota_is_stable_across_deduplicated_blob_migration
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_rollback_window
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_claims_due_old_placements_deterministically
  - functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary
---

# Signature

`async fn create_verified_migration_job( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, blob_id: Uuid, target_pool_id: Uuid, ) -> BlobMigrationJob`

# Calls

- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [copy_and_verify_one_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)

# Called by

- [switch_verified_migration_job_leaves_one_active_target_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_verified_migration_job_leaves_one_active_target_placement.md)
- [repeated_switch_verified_migration_job_is_idempotent](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/repeated_switch_verified_migration_job_is_idempotent.md)
- [switch_preserves_reads_stats_and_verification_across_phases](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases.md)
- [switch_writes_rollback_window_to_retiring_source_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_writes_rollback_window_to_retiring_source_placement.md)
- [logical_quota_is_stable_across_deduplicated_blob_migration](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/logical_quota_is_stable_across_deduplicated_blob_migration.md)
- [retiring_placement_cleanup_is_blocked_by_rollback_window](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_rollback_window.md)
- [retiring_placement_cleanup_is_blocked_when_live_references_need_it](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it.md)
- [retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold.md)
- [cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads.md)
- [cleanup_worker_repeated_execution_is_idempotent](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent.md)
- [cleanup_worker_records_retryable_failure_without_breaking_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads.md)
- [cleanup_worker_claims_due_old_placements_deterministically](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_claims_due_old_placements_deterministically.md)
- [attachment_content_fetch_reads_through_blob_store_boundary](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary.md)