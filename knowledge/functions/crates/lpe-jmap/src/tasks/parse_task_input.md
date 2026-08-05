---
type: Rust Function
title: parse_task_input
resource: crates/lpe-jmap/src/tasks.rs#L567-L605
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tasks/reject_unknown_task_properties
  - functions/crates/lpe-jmap/src/tasks/validate_task_list_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/parse/parse_required_string
  - functions/crates/lpe-jmap/src/parse/parse_optional_string
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set
---

# Signature

`fn parse_task_input( id: Option<Uuid>, account_id: Uuid, value: Value, ) -> Result<UpsertClientTaskInput>`

# Calls

- [reject_unknown_task_properties](../../../../../functions/crates/lpe-jmap/src/tasks/reject_unknown_task_properties.md)
- [validate_task_list_id](../../../../../functions/crates/lpe-jmap/src/tasks/validate_task_list_id.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_required_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_required_string.md)
- [parse_optional_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_optional_string.md)

# Called by

- [handle_task_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set.md)