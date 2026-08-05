---
type: Rust Function
title: storage_pool_summary_from_row
resource: crates/lpe-storage/src/storage_policy.rs#L803-L805
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/pool_row_from_row
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool
---

# Signature

`fn storage_pool_summary_from_row(row: sqlx::postgres::PgRow) -> Result<StoragePoolSummary>`

# Calls

- [pool_row_from_row](../../../../../functions/crates/lpe-storage/src/storage_policy/pool_row_from_row.md)

# Called by

- [create_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool.md)
- [update_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool.md)