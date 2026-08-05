---
type: Rust Method
title: fetch_platform_storage_migrations
resource: crates/lpe-storage/src/storage_visibility.rs#L71-L75
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_migrations
---

# Signature

`pub async fn fetch_platform_storage_migrations( &self, ) -> Result<StorageMigrationVisibilityResponse>`

# Calls

- [fetch_storage_migrations](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations.md)

# Called by

- [get_storage_migrations](../../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_migrations.md)