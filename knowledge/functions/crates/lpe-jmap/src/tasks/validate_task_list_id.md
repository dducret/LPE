---
type: Rust Function
title: validate_task_list_id
resource: crates/lpe-jmap/src/tasks.rs#L679-L687
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  called_by:
  - functions/crates/lpe-jmap/src/tasks/parse_task_input
---

# Signature

`fn validate_task_list_id(value: Option<&Value>) -> Result<Option<Uuid>>`

# Calls

- [parse_uuid](../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)

# Called by

- [parse_task_input](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_input.md)