---
type: Rust Method
title: fetch_platform_storage_policy_overview
resource: crates/lpe-storage/src/storage_policy.rs#L168-L170
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview
  called_by:
  - functions/crates/lpe-admin-api/src/storage/get_storage_policies
  - functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy
  - functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin
---

# Signature

`pub async fn fetch_platform_storage_policy_overview(&self) -> Result<StoragePolicyOverview>`

# Calls

- [fetch_storage_policy_overview](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview.md)

# Called by

- [get_storage_policies](../../../../../../functions/crates/lpe-admin-api/src/storage/get_storage_policies.md)
- [update_platform_storage_policy](../../../../../../functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy.md)
- [storage_policy_response_for_admin](../../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin.md)