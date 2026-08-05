---
type: Rust Function
title: get_storage_policies
resource: crates/lpe-admin-api/src/storage.rs#L81-L103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/storage/is_global_storage_admin
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview
  - functions/crates/lpe-admin-api/src/storage/admin_tenant_id
  - functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview
---

# Signature

`pub(crate) async fn get_storage_policies( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<StoragePolicyOverview>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [is_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/is_global_storage_admin.md)
- [fetch_platform_storage_policy_overview](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview.md)
- [admin_tenant_id](../../../../../functions/crates/lpe-admin-api/src/storage/admin_tenant_id.md)
- [ensure_tenant_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin.md)
- [fetch_tenant_storage_policy_overview](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview.md)