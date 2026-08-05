---
type: Rust Method
title: simulate_old_placement_cleanup_failure
resource: crates/lpe-storage/src/blob_store.rs#L1213-L1221
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads
---

# Signature

`async fn simulate_old_placement_cleanup_failure( &self, pool: &PgPool, placement_id: Uuid, error: &str, ) -> Result<PlacementCleanupResult>`

# Calls

- [cleanup_one_old_placement_inner](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner.md)

# Called by

- [cleanup_worker_records_retryable_failure_without_breaking_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads.md)