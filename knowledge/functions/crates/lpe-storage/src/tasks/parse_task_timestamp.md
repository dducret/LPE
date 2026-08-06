---
type: Rust Function
title: parse_task_timestamp
resource: crates/lpe-storage/src/tasks.rs#L1528-L1537
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task
---

# Signature

`fn parse_task_timestamp(value: Option<&str>, field: &str) -> Result<Option<DateTime<FixedOffset>>>`

# Called by

- [upsert_client_task](../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)