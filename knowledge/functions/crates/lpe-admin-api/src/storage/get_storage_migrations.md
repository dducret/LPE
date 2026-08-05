---
type: Rust Function
title: get_storage_migrations
resource: crates/lpe-admin-api/src/storage.rs#L129-L151
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/storage/is_global_storage_admin
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_migrations
  - functions/crates/lpe-admin-api/src/storage/admin_tenant_id
  - functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_migrations
---

# Signature

`pub(crate) async fn get_storage_migrations( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<StorageMigrationVisibilityResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [is_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/is_global_storage_admin.md)
- [fetch_platform_storage_migrations](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_migrations.md)
- [admin_tenant_id](../../../../../functions/crates/lpe-admin-api/src/storage/admin_tenant_id.md)
- [ensure_tenant_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin.md)
- [fetch_tenant_storage_migrations](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_migrations.md)