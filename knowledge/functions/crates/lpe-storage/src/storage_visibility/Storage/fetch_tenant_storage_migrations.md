---
type: Rust Method
title: fetch_tenant_storage_migrations
resource: crates/lpe-storage/src/storage_visibility.rs#L77-L83
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/ensure_visibility_tenant_exists
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_migrations
---

# Signature

`pub async fn fetch_tenant_storage_migrations( &self, tenant_id: Uuid, ) -> Result<StorageMigrationVisibilityResponse>`

# Calls

- [ensure_visibility_tenant_exists](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/ensure_visibility_tenant_exists.md)
- [fetch_storage_migrations](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_migrations.md)

# Called by

- [get_storage_migrations](../../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_migrations.md)