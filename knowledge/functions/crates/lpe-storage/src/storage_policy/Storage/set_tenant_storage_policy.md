---
type: Rust Method
title: set_tenant_storage_policy
resource: crates/lpe-storage/src/storage_policy.rs#L193-L212
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/Storage/ensure_tenant_exists
  - functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment
  called_by:
  - functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear
  - functions/crates/lpe-storage/src/storage_policy/policy_rejects_disabled_or_unknown_pool
  - functions/crates/lpe-storage/src/storage_policy/policy_changes_do_not_create_migration_jobs
  - functions/crates/lpe-storage/src/storage_policy/policy_change_records_admin_audit_event
---

# Signature

`pub async fn set_tenant_storage_policy( &self, tenant_id: Uuid, update: StoragePolicyUpdate, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [ensure_tenant_exists](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_tenant_exists.md)
- [ensure_active_storage_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool.md)
- [replace_storage_policy_assignment](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment.md)

# Called by

- [update_tenant_storage_policy](../../../../../../functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy.md)
- [tenant_domain_and_account_policy_inherit_and_clear](../../../../../../functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear.md)
- [policy_rejects_disabled_or_unknown_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/policy_rejects_disabled_or_unknown_pool.md)
- [policy_changes_do_not_create_migration_jobs](../../../../../../functions/crates/lpe-storage/src/storage_policy/policy_changes_do_not_create_migration_jobs.md)
- [policy_change_records_admin_audit_event](../../../../../../functions/crates/lpe-storage/src/storage_policy/policy_change_records_admin_audit_event.md)