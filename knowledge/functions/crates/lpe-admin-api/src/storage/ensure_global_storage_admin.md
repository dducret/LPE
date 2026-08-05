---
type: Rust Function
title: ensure_global_storage_admin
resource: crates/lpe-admin-api/src/storage.rs#L353-L364
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/storage/is_global_storage_admin
  called_by:
  - functions/crates/lpe-admin-api/src/storage/create_storage_pool
  - functions/crates/lpe-admin-api/src/storage/update_storage_pool
  - functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy
---

# Signature

`fn ensure_global_storage_admin( admin: &AuthenticatedAdmin, ) -> std::result::Result<(), (StatusCode, String)>`

# Calls

- [is_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/is_global_storage_admin.md)

# Called by

- [create_storage_pool](../../../../../functions/crates/lpe-admin-api/src/storage/create_storage_pool.md)
- [update_storage_pool](../../../../../functions/crates/lpe-admin-api/src/storage/update_storage_pool.md)
- [update_platform_storage_policy](../../../../../functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy.md)