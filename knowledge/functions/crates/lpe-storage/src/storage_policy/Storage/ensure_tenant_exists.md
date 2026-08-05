---
type: Rust Method
title: ensure_tenant_exists
resource: crates/lpe-storage/src/storage_policy.rs#L681-L699
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy
---

# Signature

`async fn ensure_tenant_exists(&self, tenant_id: Uuid) -> Result<()>`

# Called by

- [fetch_tenant_storage_policy_overview](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview.md)
- [set_tenant_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)