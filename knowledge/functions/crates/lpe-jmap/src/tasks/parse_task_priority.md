---
type: Rust Function
title: parse_task_priority
resource: crates/lpe-jmap/src/tasks.rs#L613-L624
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  called_by:
  - functions/crates/lpe-jmap/src/tasks/parse_task_input
---

# Signature

`fn parse_task_priority(value: Option<&Value>) -> Result<i32>`

# Calls

- [as_i64](../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)

# Called by

- [parse_task_input](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_input.md)