---
type: Rust Function
title: ensure_tenant_storage_admin
resource: crates/lpe-admin-api/src/storage.rs#L366-L382
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/storage/is_global_storage_admin
  - functions/crates/lpe-admin-api/src/storage/admin_tenant_id
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_policies
  - functions/crates/lpe-admin-api/src/storage/get_storage_health
  - functions/crates/lpe-admin-api/src/storage/get_storage_migrations
  - functions/crates/lpe-admin-api/src/storage/get_storage_cleanup
  - functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy
  - functions/crates/lpe-admin-api/src/storage/update_account_storage_policy
---

# Signature

`fn ensure_tenant_storage_admin( admin: &AuthenticatedAdmin, tenant_id: Uuid, ) -> std::result::Result<(), (StatusCode, String)>`

# Calls

- [is_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/is_global_storage_admin.md)
- [admin_tenant_id](../../../../../functions/crates/lpe-admin-api/src/storage/admin_tenant_id.md)

# Called by

- [get_storage_policies](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_policies.md)
- [get_storage_health](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_health.md)
- [get_storage_migrations](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_migrations.md)
- [get_storage_cleanup](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_cleanup.md)
- [update_tenant_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy.md)
- [update_domain_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy.md)
- [update_account_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_account_storage_policy.md)