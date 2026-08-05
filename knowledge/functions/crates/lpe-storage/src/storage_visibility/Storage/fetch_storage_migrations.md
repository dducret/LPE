---
type: Rust Method
title: fetch_storage_migrations
resource: crates/lpe-storage/src/storage_visibility.rs#L191-L255
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/load_migration_counts
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_visibility/pool_reference_from_columns
  - functions/crates/lpe-storage/src/storage_visibility/summarize_error
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_migrations
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_migrations
---

# Signature

`async fn fetch_storage_migrations( &self, tenant_filter: Option<Uuid>, ) -> Result<StorageMigrationVisibilityResponse>`

# Calls

- [load_migration_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/load_migration_counts.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool_reference_from_columns](../../../../../../functions/crates/lpe-storage/src/storage_visibility/pool_reference_from_columns.md)
- [summarize_error](../../../../../../functions/crates/lpe-storage/src/storage_visibility/summarize_error.md)

# Called by

- [fetch_platform_storage_migrations](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_migrations.md)
- [fetch_tenant_storage_migrations](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_migrations.md)