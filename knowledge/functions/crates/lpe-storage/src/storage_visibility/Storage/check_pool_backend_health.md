---
type: Rust Method
title: check_pool_backend_health
resource: crates/lpe-storage/src/storage_visibility.rs#L756-L797
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_pool_probe_placement
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
  - functions/crates/lpe-storage/src/storage_backend/s3_probe_pool
  - functions/crates/lpe-storage/src/storage_visibility/pool_backend_health_from_result
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/pool_health_summaries
---

# Signature

`async fn check_pool_backend_health(&self, row: &PoolHealthRow) -> Result<PoolBackendHealth>`

# Calls

- [select_storage_backend](../../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)
- [load_pool_probe_placement](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_pool_probe_placement.md)
- [s3_stat_object](../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)
- [s3_probe_pool](../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_probe_pool.md)
- [pool_backend_health_from_result](../../../../../../functions/crates/lpe-storage/src/storage_visibility/pool_backend_health_from_result.md)

# Called by

- [pool_health_summaries](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/pool_health_summaries.md)