---
type: Rust Function
title: expect_anyhow_failure
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L664-L667
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_name_policy_storage_guards
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_managed_retention_folder_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path
---

# Signature

`fn expect_anyhow_failure<T>(label: &str, result: Result<T>) -> Result<()>`

# Called by

- [exercise_mailbox_name_policy_storage_guards](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_name_policy_storage_guards.md)
- [exercise_managed_retention_folder_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_managed_retention_folder_path.md)
- [exercise_custom_calendar_grant_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path.md)