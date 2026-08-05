---
type: Rust Function
title: pool_reference_from_columns
resource: crates/lpe-storage/src/storage_visibility.rs#L943-L953
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_rows
---

# Signature

`fn pool_reference_from_columns( row: &sqlx::postgres::PgRow, prefix: &str, ) -> Result<StoragePoolReference>`

# Called by

- [fetch_storage_migrations](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations.md)
- [load_cleanup_rows](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_cleanup_rows.md)