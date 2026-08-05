---
type: Rust Method
title: cleanup_one_old_placement_inner
resource: crates/lpe-storage/src/blob_store.rs#L1039-L1169
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/placement_status
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/record_placement_cleanup_failure
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/simulate_old_placement_cleanup_failure
---

# Signature

`async fn cleanup_one_old_placement_inner( &self, pool: &PgPool, placement_id: Uuid, forced_error: Option<&str>, ) -> Result<PlacementCleanupResult>`

# Calls

- [placement_status](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/placement_status.md)
- [old_placement_cleanup_eligibility](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility.md)
- [record_placement_cleanup_failure](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/record_placement_cleanup_failure.md)

# Called by

- [cleanup_one_old_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement.md)
- [simulate_old_placement_cleanup_failure](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/simulate_old_placement_cleanup_failure.md)