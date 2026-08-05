---
type: Rust Function
title: parse_task_list_update
resource: crates/lpe-jmap/src/tasks.rs#L619-L637
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tasks/reject_unknown_task_list_properties
  - functions/crates/lpe-jmap/src/parse/parse_optional_string
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
---

# Signature

`fn parse_task_list_update( account_id: Uuid, task_list_id: Uuid, value: Value, ) -> Result<UpdateTaskListInput>`

# Calls

- [reject_unknown_task_list_properties](../../../../../functions/crates/lpe-jmap/src/tasks/reject_unknown_task_list_properties.md)
- [parse_optional_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_optional_string.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_task_list_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)