---
type: Rust Method
title: if_empty
resource: crates/lpe-exchange/src/service/ews/fields.rs#L8-L14
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
---

# Signature

`fn if_empty(self, fallback: String) -> String`

# Called by

- [parse_update_event_input](../../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [parse_update_task_input](../../../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)