---
type: Rust Method
title: update_jmap_task_list
resource: crates/lpe-jmap/src/store.rs#L1071-L1073
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/update_task_list
---

# Signature

`async fn update_jmap_task_list(&self, input: UpdateTaskListInput) -> Result<ClientTaskList>`

# Calls

- [update_task_list](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_task_list.md)