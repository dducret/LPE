---
type: Rust Method
title: load_migration_counts
resource: crates/lpe-storage/src/storage_visibility.rs#L404-L449
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations
---

# Signature

`async fn load_migration_counts( &self, tenant_filter: Option<Uuid>, ) -> Result<StorageMigrationCounts>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [fetch_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health.md)
- [fetch_storage_migrations](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations.md)