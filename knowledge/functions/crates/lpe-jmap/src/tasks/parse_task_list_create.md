---
type: Rust Function
title: parse_task_list_create
resource: crates/lpe-jmap/src/tasks.rs#L626-L636
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tasks/reject_unknown_task_list_properties
  - functions/crates/lpe-jmap/src/parse/parse_required_string
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
---

# Signature

`fn parse_task_list_create(account_id: Uuid, value: Value) -> Result<CreateTaskListInput>`

# Calls

- [reject_unknown_task_list_properties](../../../../../functions/crates/lpe-jmap/src/tasks/reject_unknown_task_list_properties.md)
- [parse_required_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_required_string.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_task_list_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)