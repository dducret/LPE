---
type: Rust Function
title: storage_audit
resource: crates/lpe-admin-api/src/storage.rs#L317-L323
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/storage/create_storage_pool
  - functions/crates/lpe-admin-api/src/storage/update_storage_pool
  - functions/crates/lpe-admin-api/src/storage/storage_policy_audit
---

# Signature

`fn storage_audit(admin: &AuthenticatedAdmin, action: &str, subject: &str) -> AuditEntryInput`

# Called by

- [create_storage_pool](../../../../../functions/crates/lpe-admin-api/src/storage/create_storage_pool.md)
- [update_storage_pool](../../../../../functions/crates/lpe-admin-api/src/storage/update_storage_pool.md)
- [storage_policy_audit](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit.md)