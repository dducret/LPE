---
type: Rust Method
title: cleanup_one_old_placement
resource: crates/lpe-storage/src/blob_store.rs#L1030-L1037
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_old_retiring_placements
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent
  - functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary
  - functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source
---

# Signature

`pub(crate) async fn cleanup_one_old_placement( &self, pool: &PgPool, placement_id: Uuid, ) -> Result<PlacementCleanupResult>`

# Calls

- [cleanup_one_old_placement_inner](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner.md)

# Called by

- [cleanup_old_retiring_placements](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_old_retiring_placements.md)
- [cleanup_worker_refuses_the_only_active_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement.md)
- [cleanup_worker_repeated_execution_is_idempotent](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_repeated_execution_is_idempotent.md)
- [attachment_content_fetch_reads_through_blob_store_boundary](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary.md)
- [migrate_attachment_and_cleanup_source](../../../../../../functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source.md)