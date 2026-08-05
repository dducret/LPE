---
type: Rust Function
title: normalize_storage_pool_status
resource: crates/lpe-storage/src/storage_policy.rs#L782-L788
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool
---

# Signature

`fn normalize_storage_pool_status(status: &str) -> Result<&'static str>`

# Called by

- [create_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool.md)
- [update_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool.md)