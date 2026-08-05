---
type: Rust Method
title: storage_policy_tenant_for_domain
resource: crates/lpe-storage/src/storage_policy.rs#L258-L271
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy
---

# Signature

`pub async fn storage_policy_tenant_for_domain(&self, domain_id: Uuid) -> Result<Uuid>`

# Called by

- [update_domain_storage_policy](../../../../../../functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy.md)
- [set_domain_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy.md)