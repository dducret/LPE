---
type: Rust Method
title: fetch_tenant_storage_policy_overview
resource: crates/lpe-storage/src/storage_policy.rs#L172-L178
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/Storage/ensure_tenant_exists
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_policies
  - functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin
  - functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear
---

# Signature

`pub async fn fetch_tenant_storage_policy_overview( &self, tenant_id: Uuid, ) -> Result<StoragePolicyOverview>`

# Calls

- [ensure_tenant_exists](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_tenant_exists.md)
- [fetch_storage_policy_overview](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview.md)

# Called by

- [get_storage_policies](../../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_policies.md)
- [storage_policy_response_for_admin](../../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin.md)
- [tenant_domain_and_account_policy_inherit_and_clear](../../../../../../functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear.md)