---
type: Rust Function
title: task_list_to_value
resource: crates/lpe-jmap/src/tasks.rs#L475-L498
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
---

# Signature

`fn task_list_to_value(task_list: &ClientTaskList, properties: &HashSet<String>) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_task_list_get](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get.md)
- [handle_task_list_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)