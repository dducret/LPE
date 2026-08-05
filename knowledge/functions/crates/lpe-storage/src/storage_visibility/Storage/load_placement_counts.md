---
type: Rust Method
title: load_placement_counts
resource: crates/lpe-storage/src/storage_visibility.rs#L339-L374
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_visibility/Storage/count_missing_active_placements
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health
---

# Signature

`async fn load_placement_counts( &self, tenant_filter: Option<Uuid>, ) -> Result<StoragePlacementCounts>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [count_missing_active_placements](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/count_missing_active_placements.md)

# Called by

- [fetch_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health.md)