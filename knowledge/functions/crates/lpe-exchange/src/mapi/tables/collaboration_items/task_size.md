---
type: Rust Function
title: task_size
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L23-L28
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/sync/task_sync_object
---

# Signature

`pub(in crate::mapi) fn task_size(task: &ClientTask) -> i64`

# Called by

- [task_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)
- [task_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/task_sync_object.md)