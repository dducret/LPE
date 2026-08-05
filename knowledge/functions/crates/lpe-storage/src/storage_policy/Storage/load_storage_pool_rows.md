---
type: Rust Method
title: load_storage_pool_rows
resource: crates/lpe-storage/src/storage_policy.rs#L447-L474
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/list_storage_pools
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview
---

# Signature

`async fn load_storage_pool_rows(&self, include_disabled: bool) -> Result<Vec<PoolRow>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [list_storage_pools](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/list_storage_pools.md)
- [fetch_storage_policy_overview](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview.md)