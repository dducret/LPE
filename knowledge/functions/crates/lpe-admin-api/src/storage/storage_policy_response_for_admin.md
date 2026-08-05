---
type: Rust Function
title: storage_policy_response_for_admin
resource: crates/lpe-admin-api/src/storage.rs#L296-L315
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/storage/is_global_storage_admin
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview
  - functions/crates/lpe-admin-api/src/storage/admin_tenant_id
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview
  called_by:
  - functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_account_storage_policy
---

# Signature

`async fn storage_policy_response_for_admin( storage: &Storage, admin: &AuthenticatedAdmin, ) -> ApiResult<StoragePolicyOverview>`

# Calls

- [is_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/is_global_storage_admin.md)
- [fetch_platform_storage_policy_overview](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview.md)
- [admin_tenant_id](../../../../../functions/crates/lpe-admin-api/src/storage/admin_tenant_id.md)
- [fetch_tenant_storage_policy_overview](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview.md)

# Called by

- [update_tenant_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy.md)
- [update_domain_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy.md)
- [update_account_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_account_storage_policy.md)