---
type: Rust Method
title: ensure_active_storage_pool
resource: crates/lpe-storage/src/storage_policy.rs#L701-L708
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_row
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_platform_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy
---

# Signature

`async fn ensure_active_storage_pool(&self, pool_id: Uuid) -> Result<()>`

# Calls

- [load_storage_pool_row](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_row.md)
- [select_storage_backend](../../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)

# Called by

- [set_platform_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_platform_storage_policy.md)
- [set_tenant_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)
- [set_domain_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy.md)
- [set_account_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy.md)