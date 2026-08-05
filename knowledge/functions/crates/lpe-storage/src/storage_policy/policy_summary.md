---
type: Rust Function
title: policy_summary
resource: crates/lpe-storage/src/storage_policy.rs#L873-L892
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/assignment_pool
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview
---

# Signature

`fn policy_summary( scope: StoragePolicyScope, assignment: Option<&AssignmentRow>, effective_pool: &StoragePoolReference, inherited_from: Option<String>, pool_map: &HashMap<Uuid, StoragePoolReference>, ) -> Result<StoragePolicySummary>`

# Calls

- [assignment_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/assignment_pool.md)

# Called by

- [fetch_storage_policy_overview](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview.md)