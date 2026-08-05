---
type: Rust Function
title: normalize_storage_pool_kind
resource: crates/lpe-storage/src/storage_backend.rs#L100-L106
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  - functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool
---

# Signature

`pub(crate) fn normalize_storage_pool_kind(pool_kind: &str) -> Result<&'static str>`

# Called by

- [normalize_storage_pool_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config.md)
- [select_storage_backend](../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)
- [create_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool.md)