---
type: Rust Method
title: load_storage_policy_assignments
resource: crates/lpe-storage/src/storage_policy.rs#L504-L530
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview
---

# Signature

`async fn load_storage_policy_assignments( &self, tenant_filter: Option<Uuid>, ) -> Result<Vec<AssignmentRow>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [fetch_storage_policy_overview](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview.md)