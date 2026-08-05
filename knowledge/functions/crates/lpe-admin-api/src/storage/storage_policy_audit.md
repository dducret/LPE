---
type: Rust Function
title: storage_policy_audit
resource: crates/lpe-admin-api/src/storage.rs#L325-L343
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/storage/storage_audit
  called_by:
  - functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_account_storage_policy
  - functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_scope_and_pool_target
  - functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_inheritance_clear
---

# Signature

`fn storage_policy_audit( admin: &AuthenticatedAdmin, action: &str, scope_kind: &str, scope_id: Option<Uuid>, storage_pool_id: Option<Uuid>, ) -> AuditEntryInput`

# Calls

- [storage_audit](../../../../../functions/crates/lpe-admin-api/src/storage/storage_audit.md)

# Called by

- [update_platform_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy.md)
- [update_tenant_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy.md)
- [update_domain_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy.md)
- [update_account_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_account_storage_policy.md)
- [storage_policy_audit_records_scope_and_pool_target](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_scope_and_pool_target.md)
- [storage_policy_audit_records_inheritance_clear](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_inheritance_clear.md)