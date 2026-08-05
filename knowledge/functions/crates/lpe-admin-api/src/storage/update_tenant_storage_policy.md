---
type: Rust Function
title: update_tenant_storage_policy
resource: crates/lpe-admin-api/src/storage.rs#L207-L232
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy
  - functions/crates/lpe-admin-api/src/storage/storage_policy_audit
  - functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin
---

# Signature

`pub(crate) async fn update_tenant_storage_policy( State(storage): State<Storage>, headers: HeaderMap, AxumPath(tenant_id): AxumPath<Uuid>, Json(request): Json<UpdateStoragePolicyRequest>, ) -> ApiResult<StoragePolicyOverview>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [ensure_tenant_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin.md)
- [set_tenant_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)
- [storage_policy_audit](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit.md)
- [storage_policy_response_for_admin](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin.md)