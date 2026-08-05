---
type: Rust Function
title: expire_retiring_placement
resource: crates/lpe-storage/src/blob_store/tests.rs#L282-L297
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary
---

# Signature

`async fn expire_retiring_placement(storage: &Storage, tenant_id: Uuid, placement_id: Uuid)`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [retiring_placement_cleanup_is_blocked_when_live_references_need_it](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it.md)
- [retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold.md)
- [cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads.md)
- [cleanup_worker_repeated_execution_is_idempotent](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent.md)
- [cleanup_worker_records_retryable_failure_without_breaking_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads.md)
- [attachment_content_fetch_reads_through_blob_store_boundary](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary.md)