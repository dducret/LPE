---
type: Rust Function
title: update_account_storage_policy
resource: crates/lpe-admin-api/src/storage.rs#L265-L294
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/storage_policy/Storage/storage_policy_tenant_and_domain_for_account
  - functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy
  - functions/crates/lpe-admin-api/src/storage/storage_policy_audit
  - functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin
---

# Signature

`pub(crate) async fn update_account_storage_policy( State(storage): State<Storage>, headers: HeaderMap, AxumPath(account_id): AxumPath<Uuid>, Json(request): Json<UpdateStoragePolicyRequest>, ) -> ApiResult<StoragePolicyOverview>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [storage_policy_tenant_and_domain_for_account](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/storage_policy_tenant_and_domain_for_account.md)
- [ensure_tenant_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin.md)
- [set_account_storage_policy](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy.md)
- [storage_policy_audit](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit.md)
- [storage_policy_response_for_admin](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin.md)