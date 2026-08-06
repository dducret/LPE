---
type: Rust Method
title: fetch_jmap_tasks_by_ids
resource: crates/lpe-jmap/src/store.rs#L1109-L1115
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids
---

# Signature

`async fn fetch_jmap_tasks_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<ClientTask>>`

# Calls

- [fetch_client_tasks_by_ids](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids.md)