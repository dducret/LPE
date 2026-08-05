---
type: Rust Function
title: pool_health_summary
resource: crates/lpe-storage/src/storage_visibility.rs#L850-L882
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/pool_health_summaries
  - functions/crates/lpe-storage/src/storage_visibility/tests/pool_health_marks_failed_placements_degraded
---

# Signature

`fn pool_health_summary(row: PoolHealthRow, backend: PoolBackendHealth) -> StoragePoolHealth`

# Called by

- [pool_health_summaries](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/pool_health_summaries.md)
- [pool_health_marks_failed_placements_degraded](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/pool_health_marks_failed_placements_degraded.md)