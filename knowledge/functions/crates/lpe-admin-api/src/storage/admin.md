---
type: Rust Function
title: admin
resource: crates/lpe-admin-api/src/storage.rs#L410-L423
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/storage/global_admin_can_manage_platform_storage_policy
  - functions/crates/lpe-admin-api/src/storage/tenant_admin_is_limited_to_own_tenant_storage_policy
  - functions/crates/lpe-admin-api/src/storage/storage_visibility_uses_global_or_own_tenant_scope
  - functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_scope_and_pool_target
  - functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_inheritance_clear
---

# Signature

`fn admin(role: &str, tenant_id: Uuid, permissions: Vec<&str>) -> AuthenticatedAdmin`

# Called by

- [global_admin_can_manage_platform_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/global_admin_can_manage_platform_storage_policy.md)
- [tenant_admin_is_limited_to_own_tenant_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/tenant_admin_is_limited_to_own_tenant_storage_policy.md)
- [storage_visibility_uses_global_or_own_tenant_scope](../../../../../functions/crates/lpe-admin-api/src/storage/storage_visibility_uses_global_or_own_tenant_scope.md)
- [storage_policy_audit_records_scope_and_pool_target](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_scope_and_pool_target.md)
- [storage_policy_audit_records_inheritance_clear](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_inheritance_clear.md)