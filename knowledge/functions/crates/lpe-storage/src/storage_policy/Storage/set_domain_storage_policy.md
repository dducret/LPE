---
type: Rust Method
title: set_domain_storage_policy
resource: crates/lpe-storage/src/storage_policy.rs#L214-L233
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/Storage/storage_policy_tenant_for_domain
  - functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment
  called_by:
  - functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear
---

# Signature

`pub async fn set_domain_storage_policy( &self, domain_id: Uuid, update: StoragePolicyUpdate, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [storage_policy_tenant_for_domain](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/storage_policy_tenant_for_domain.md)
- [ensure_active_storage_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool.md)
- [replace_storage_policy_assignment](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment.md)

# Called by

- [update_domain_storage_policy](../../../../../../functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy.md)
- [tenant_domain_and_account_policy_inherit_and_clear](../../../../../../functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear.md)