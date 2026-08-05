---
type: Rust Function
title: ews_task_status_to_canonical
resource: crates/lpe-exchange/src/service/ews/tasks.rs#L188-L190
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/ews_types/EwsTaskStatus/canonical_status
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
---

# Signature

`fn ews_task_status_to_canonical(value: &str) -> Result<&'static str>`

# Calls

- [canonical_status](../../../../../../../functions/crates/lpe-exchange/src/ews_types/EwsTaskStatus/canonical_status.md)

# Called by

- [parse_create_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input.md)
- [parse_update_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)