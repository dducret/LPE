---
type: Rust Function
title: update_platform_storage_policy
resource: crates/lpe-admin-api/src/storage.rs#L177-L205
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_platform_storage_policy
  - functions/crates/lpe-admin-api/src/storage/storage_policy_audit
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview
---

# Signature

`pub(crate) async fn update_platform_storage_policy( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpdateStoragePolicyRequest>, ) -> ApiResult<StoragePolicyOverview>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [ensure_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin.md)
- [set_platform_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_platform_storage_policy.md)
- [storage_policy_audit](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit.md)
- [fetch_platform_storage_policy_overview](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview.md)