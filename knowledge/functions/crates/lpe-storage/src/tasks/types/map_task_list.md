---
type: Rust Function
title: map_task_list
resource: crates/lpe-storage/src/tasks/types.rs#L124-L142
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/tasks/Storage/create_task_list
  - functions/crates/lpe-storage/src/tasks/Storage/update_task_list
---

# Signature

`pub(crate) fn map_task_list(row: ClientTaskListRow) -> ClientTaskList`

# Called by

- [create_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/create_task_list.md)
- [update_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_task_list.md)