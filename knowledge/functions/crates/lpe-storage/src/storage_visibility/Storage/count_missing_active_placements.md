---
type: Rust Method
title: count_missing_active_placements
resource: crates/lpe-storage/src/storage_visibility.rs#L376-L402
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_placement_counts
---

# Signature

`async fn count_missing_active_placements(&self, tenant_filter: Option<Uuid>) -> Result<u64>`

# Called by

- [fetch_storage_metadata_diagnostics](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics.md)
- [load_placement_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_placement_counts.md)