---
type: Rust Function
title: is_global_storage_admin
resource: crates/lpe-admin-api/src/storage.rs#L345-L351
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/storage/list_storage_pools
  - functions/crates/lpe-admin-api/src/storage/get_storage_policies
  - functions/crates/lpe-admin-api/src/storage/get_storage_health
  - functions/crates/lpe-admin-api/src/storage/get_storage_migrations
  - functions/crates/lpe-admin-api/src/storage/get_storage_cleanup
  - functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin
  - functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin
  - functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin
---

# Signature

`fn is_global_storage_admin(admin: &AuthenticatedAdmin) -> bool`

# Called by

- [list_storage_pools](../../../../../functions/crates/lpe-admin-api/src/storage/list_storage_pools.md)
- [get_storage_policies](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_policies.md)
- [get_storage_health](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_health.md)
- [get_storage_migrations](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_migrations.md)
- [get_storage_cleanup](../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_cleanup.md)
- [storage_policy_response_for_admin](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin.md)
- [ensure_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin.md)
- [ensure_tenant_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin.md)