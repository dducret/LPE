---
type: Rust Method
title: replace_storage_policy_assignment
resource: crates/lpe-storage/src/storage_policy.rs#L626-L679
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_platform_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy
  - functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy
---

# Signature

`async fn replace_storage_policy_assignment( &self, scope_kind: &str, tenant_id: Option<Uuid>, domain_id: Option<Uuid>, account_id: Option<Uuid>, storage_pool_id: Option<Uuid>, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)

# Called by

- [set_platform_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_platform_storage_policy.md)
- [set_tenant_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)
- [set_domain_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy.md)
- [set_account_storage_policy](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy.md)