---
type: Rust Method
title: load_pool_probe_placement
resource: crates/lpe-storage/src/storage_visibility.rs#L799-L827
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health
---

# Signature

`async fn load_pool_probe_placement(&self, pool_id: Uuid) -> Result<Option<PoolProbePlacement>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [check_pool_backend_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health.md)