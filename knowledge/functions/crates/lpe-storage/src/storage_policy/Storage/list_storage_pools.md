---
type: Rust Method
title: list_storage_pools
resource: crates/lpe-storage/src/storage_policy.rs#L63-L69
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_rows
---

# Signature

`pub async fn list_storage_pools( &self, include_disabled: bool, ) -> Result<Vec<StoragePoolSummary>>`

# Calls

- [load_storage_pool_rows](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_rows.md)