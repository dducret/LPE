---
type: Rust Method
title: storage_policy_tenant_and_domain_for_account
resource: crates/lpe-storage/src/storage_policy.rs#L273-L289
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/storage/update_account_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy
---

# Signature

`pub async fn storage_policy_tenant_and_domain_for_account( &self, account_id: Uuid, ) -> Result<(Uuid, Uuid)>`

# Called by

- [update_account_storage_policy](../../../../../../functions/crates/lpe-admin-api/src/storage/update_account_storage_policy.md)
- [set_account_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy.md)