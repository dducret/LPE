---
type: Rust Function
title: pool_backend_health_from_result
resource: crates/lpe-storage/src/storage_visibility.rs#L884-L892
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/pool_backend_health_from_error
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health
---

# Signature

`fn pool_backend_health_from_result(result: Result<()>) -> PoolBackendHealth`

# Calls

- [pool_backend_health_from_error](../../../../../functions/crates/lpe-storage/src/storage_visibility/pool_backend_health_from_error.md)

# Called by

- [check_pool_backend_health](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health.md)