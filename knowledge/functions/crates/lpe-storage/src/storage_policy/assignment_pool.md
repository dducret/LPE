---
type: Rust Function
title: assignment_pool
resource: crates/lpe-storage/src/storage_policy.rs#L866-L871
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview
  - functions/crates/lpe-storage/src/storage_policy/policy_summary
---

# Signature

`fn assignment_pool( assignment: Option<&AssignmentRow>, pool_map: &HashMap<Uuid, StoragePoolReference>, ) -> Option<StoragePoolReference>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [fetch_storage_policy_overview](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview.md)
- [policy_summary](../../../../../functions/crates/lpe-storage/src/storage_policy/policy_summary.md)