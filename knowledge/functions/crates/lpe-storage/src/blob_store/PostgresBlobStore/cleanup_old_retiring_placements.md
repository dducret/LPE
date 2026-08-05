---
type: Rust Method
title: cleanup_old_retiring_placements
resource: crates/lpe-storage/src/blob_store.rs#L1000-L1027
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_claims_due_old_placements_deterministically
---

# Signature

`pub(crate) async fn cleanup_old_retiring_placements( &self, pool: &PgPool, limit: i64, ) -> Result<Vec<PlacementCleanupResult>>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [cleanup_one_old_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement.md)

# Called by

- [cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads.md)
- [cleanup_worker_repeated_execution_is_idempotent](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent.md)
- [cleanup_worker_records_retryable_failure_without_breaking_active_reads](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_records_retryable_failure_without_breaking_active_reads.md)
- [cleanup_worker_claims_due_old_placements_deterministically](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_claims_due_old_placements_deterministically.md)