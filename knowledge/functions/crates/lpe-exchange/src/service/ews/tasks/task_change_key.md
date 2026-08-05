---
type: Rust Function
title: task_change_key
resource: crates/lpe-exchange/src/service/ews/tasks.rs#L3-L5
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys
---

# Signature

`pub(in crate::service) fn task_change_key(task: &ClientTask, version: &str) -> String`

# Calls

- [versioned_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key.md)

# Called by

- [task_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys.md)