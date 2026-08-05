---
type: Rust Method
title: fetch_storage_policy_overview
resource: crates/lpe-storage/src/storage_policy.rs#L291-L445
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_rows
  - functions/crates/lpe-storage/src/storage_policy/storage_pool_reference
  - functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_assignments
  - functions/crates/lpe-storage/src/storage_policy/assignment_key
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/storage_policy/policy_summary
  - functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_tenants
  - functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_domains
  - functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_accounts
  - functions/crates/lpe-storage/src/storage_policy/assignment_pool
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview
---

# Signature

`async fn fetch_storage_policy_overview( &self, tenant_filter: Option<Uuid>, ) -> Result<StoragePolicyOverview>`

# Calls

- [load_storage_pool_rows](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_rows.md)
- [storage_pool_reference](../../../../../../functions/crates/lpe-storage/src/storage_policy/storage_pool_reference.md)
- [load_storage_policy_assignments](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_assignments.md)
- [assignment_key](../../../../../../functions/crates/lpe-storage/src/storage_policy/assignment_key.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [policy_summary](../../../../../../functions/crates/lpe-storage/src/storage_policy/policy_summary.md)
- [load_storage_policy_tenants](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_tenants.md)
- [load_storage_policy_domains](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_domains.md)
- [load_storage_policy_accounts](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_accounts.md)
- [assignment_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/assignment_pool.md)

# Called by

- [fetch_platform_storage_policy_overview](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview.md)
- [fetch_tenant_storage_policy_overview](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview.md)