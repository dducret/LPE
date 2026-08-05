---
type: Rust Function
title: normalize_task_list_name
resource: crates/lpe-storage/src/util.rs#L45-L51
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/tasks/Storage/create_task_list
---

# Signature

`pub(crate) fn normalize_task_list_name(value: &str) -> Result<String>`

# Called by

- [create_task_list](../../../../../functions/crates/lpe-storage/src/tasks/Storage/create_task_list.md)