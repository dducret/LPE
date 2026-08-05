---
type: Rust Method
title: create_jmap_task_list
resource: crates/lpe-jmap/src/store.rs#L1067-L1069
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/create_task_list
---

# Signature

`async fn create_jmap_task_list(&self, input: CreateTaskListInput) -> Result<ClientTaskList>`

# Calls

- [create_task_list](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/create_task_list.md)