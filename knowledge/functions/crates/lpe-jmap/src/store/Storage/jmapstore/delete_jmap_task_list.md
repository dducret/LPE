---
type: Rust Method
title: delete_jmap_task_list
resource: crates/lpe-jmap/src/store.rs#L1075-L1077
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/delete_task_list
---

# Signature

`async fn delete_jmap_task_list(&self, account_id: Uuid, task_list_id: Uuid) -> Result<()>`

# Calls

- [delete_task_list](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list.md)