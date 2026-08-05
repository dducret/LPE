---
type: Rust Function
title: pool_backend_health_from_error
resource: crates/lpe-storage/src/storage_visibility.rs#L894-L941
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/pool_backend_health_from_result
  - functions/crates/lpe-storage/src/storage_visibility/tests/s3_backend_health_errors_map_to_provider_neutral_states
---

# Signature

`fn pool_backend_health_from_error(error: &Error) -> PoolBackendHealth`

# Called by

- [pool_backend_health_from_result](../../../../../functions/crates/lpe-storage/src/storage_visibility/pool_backend_health_from_result.md)
- [s3_backend_health_errors_map_to_provider_neutral_states](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/s3_backend_health_errors_map_to_provider_neutral_states.md)