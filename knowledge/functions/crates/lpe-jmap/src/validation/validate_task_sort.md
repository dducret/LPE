---
type: Rust Function
title: validate_task_sort
resource: crates/lpe-jmap/src/validation.rs#L63-L72
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes
---

# Signature

`pub(crate) fn validate_task_sort(sort: Option<&[TaskQuerySort]>) -> Result<()>`

# Called by

- [handle_task_query](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query.md)
- [handle_task_query_changes](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes.md)