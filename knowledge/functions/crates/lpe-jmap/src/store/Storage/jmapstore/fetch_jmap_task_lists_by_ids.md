---
type: Rust Method
title: fetch_jmap_task_lists_by_ids
resource: crates/lpe-jmap/src/store.rs#L1085-L1091
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids
---

# Signature

`async fn fetch_jmap_task_lists_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<ClientTaskList>>`

# Calls

- [fetch_task_lists_by_ids](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids.md)