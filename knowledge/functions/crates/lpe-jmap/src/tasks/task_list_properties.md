---
type: Rust Function
title: task_list_properties
resource: crates/lpe-jmap/src/tasks.rs#L435-L450
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
---

# Signature

`fn task_list_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_task_list_get](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get.md)
- [handle_task_list_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)