---
type: Rust Function
title: task_to_value
resource: crates/lpe-jmap/src/tasks.rs#L500-L537
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get
---

# Signature

`fn task_to_value(task: &ClientTask, properties: &HashSet<String>) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_task_get](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get.md)