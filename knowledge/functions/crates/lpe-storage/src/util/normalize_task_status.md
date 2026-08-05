---
type: Rust Function
title: normalize_task_status
resource: crates/lpe-storage/src/util.rs#L35-L43
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task
---

# Signature

`pub(crate) fn normalize_task_status(value: &str) -> Result<&'static str>`

# Called by

- [upsert_client_task](../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)