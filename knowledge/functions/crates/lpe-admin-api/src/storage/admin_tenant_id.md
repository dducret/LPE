---
type: Rust Function
title: admin_tenant_id
resource: crates/lpe-admin-api/src/storage.rs#L384-L386
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_policies
  - functions/crates/lpe-admin-api/src/storage/get_storage_health
  - functions/crates/lpe-admin-api/src/storage/get_storage_migrations
  - functions/crates/lpe-admin-api/src/storage/get_storage_cleanup
  - functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin
  - functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin
---

# Signature

`fn admin_tenant_id(admin: &AuthenticatedAdmin) -> std::result::Result<Uuid, (StatusCode, String)>`

# Called by

- [get_storage_policies](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_policies.md)
- [get_storage_health](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_health.md)
- [get_storage_migrations](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_migrations.md)
- [get_storage_cleanup](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_cleanup.md)
- [storage_policy_response_for_admin](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin.md)
- [ensure_tenant_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin.md)