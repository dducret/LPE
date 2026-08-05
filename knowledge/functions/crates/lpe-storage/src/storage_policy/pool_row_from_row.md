---
type: Rust Function
title: pool_row_from_row
resource: crates/lpe-storage/src/storage_policy.rs#L790-L801
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_row
  - functions/crates/lpe-storage/src/storage_policy/storage_pool_summary_from_row
---

# Signature

`fn pool_row_from_row(row: sqlx::postgres::PgRow) -> Result<PoolRow>`

# Called by

- [load_storage_pool_row](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_row.md)
- [storage_pool_summary_from_row](../../../../../functions/crates/lpe-storage/src/storage_policy/storage_pool_summary_from_row.md)