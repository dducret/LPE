---
type: Rust Method
title: fetch_jmap_task_lists
resource: crates/lpe-jmap/src/store.rs#L1055-L1057
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists
---

# Signature

`async fn fetch_jmap_task_lists(&self, account_id: Uuid) -> Result<Vec<ClientTaskList>>`

# Calls

- [fetch_task_lists](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists.md)