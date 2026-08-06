---
type: Rust Function
title: reject_unknown_task_properties
resource: crates/lpe-jmap/src/tasks.rs#L658-L667
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tasks/parse_task_input
---

# Signature

`fn reject_unknown_task_properties(object: &Map<String, Value>) -> Result<()>`

# Called by

- [parse_task_input](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_input.md)