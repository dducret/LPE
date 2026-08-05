---
type: Rust Function
title: validate_task_filter
resource: crates/lpe-jmap/src/validation.rs#L74-L84
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-jmap/src/validation/validate_task_status_value
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes
---

# Signature

`pub(crate) fn validate_task_filter(filter: Option<&TaskQueryFilter>) -> Result<()>`

# Calls

- [parse_uuid](../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [validate_task_status_value](../../../../../functions/crates/lpe-jmap/src/validation/validate_task_status_value.md)

# Called by

- [handle_task_query](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query.md)
- [handle_task_query_changes](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes.md)