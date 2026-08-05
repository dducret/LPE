---
type: Rust Method
title: load_storage_pool_row
resource: crates/lpe-storage/src/storage_policy.rs#L476-L502
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_policy/pool_row_from_row
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool
---

# Signature

`async fn load_storage_pool_row(&self, pool_id: Uuid) -> Result<PoolRow>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool_row_from_row](../../../../../../functions/crates/lpe-storage/src/storage_policy/pool_row_from_row.md)

# Called by

- [update_storage_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool.md)
- [ensure_active_storage_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool.md)