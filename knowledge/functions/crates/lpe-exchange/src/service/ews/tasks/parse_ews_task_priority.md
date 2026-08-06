---
type: Rust Function
title: parse_ews_task_priority
resource: crates/lpe-exchange/src/service/ews/tasks.rs#L236-L247
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
---

# Signature

`fn parse_ews_task_priority(value: Option<String>) -> Result<i32>`

# Called by

- [parse_create_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input.md)
- [parse_update_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)