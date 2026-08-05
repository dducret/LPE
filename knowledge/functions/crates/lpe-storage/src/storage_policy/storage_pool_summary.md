---
type: Rust Function
title: storage_pool_summary
resource: crates/lpe-storage/src/storage_policy.rs#L807-L824
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/storage_pool_config_summary
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
---

# Signature

`fn storage_pool_summary(pool: PoolRow) -> StoragePoolSummary`

# Calls

- [storage_pool_config_summary](../../../../../functions/crates/lpe-storage/src/storage_backend/storage_pool_config_summary.md)
- [select_storage_backend](../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)