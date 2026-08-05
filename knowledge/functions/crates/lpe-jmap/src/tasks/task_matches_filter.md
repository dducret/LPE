---
type: Rust Function
title: task_matches_filter
resource: crates/lpe-jmap/src/tasks.rs#L535-L556
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes
---

# Signature

`fn task_matches_filter(task: &ClientTask, filter: &TaskQueryFilter) -> bool`

# Called by

- [handle_task_query](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query.md)
- [handle_task_query_changes](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes.md)