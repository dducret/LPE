---
type: Rust Function
title: reject_unknown_task_list_properties
resource: crates/lpe-jmap/src/tasks.rs#L650-L658
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tasks/parse_task_list_create
  - functions/crates/lpe-jmap/src/tasks/parse_task_list_update
---

# Signature

`fn reject_unknown_task_list_properties(object: &Map<String, Value>) -> Result<()>`

# Called by

- [parse_task_list_create](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_list_create.md)
- [parse_task_list_update](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_list_update.md)