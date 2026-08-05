---
type: Rust Method
title: set_platform_storage_policy
resource: crates/lpe-storage/src/storage_policy.rs#L180-L191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment
  called_by:
  - functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy
---

# Signature

`pub async fn set_platform_storage_policy( &self, update: StoragePolicyUpdate, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [ensure_active_storage_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool.md)
- [replace_storage_policy_assignment](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment.md)

# Called by

- [update_platform_storage_policy](../../../../../../functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy.md)