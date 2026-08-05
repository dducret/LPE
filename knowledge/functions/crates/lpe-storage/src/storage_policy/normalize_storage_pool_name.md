---
type: Rust Function
title: normalize_storage_pool_name
resource: crates/lpe-storage/src/storage_policy.rs#L774-L780
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool
---

# Signature

`fn normalize_storage_pool_name(name: &str) -> Result<String>`

# Called by

- [create_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool.md)
- [update_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool.md)