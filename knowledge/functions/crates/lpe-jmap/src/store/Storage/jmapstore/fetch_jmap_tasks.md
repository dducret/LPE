---
type: Rust Method
title: fetch_jmap_tasks
resource: crates/lpe-jmap/src/store.rs#L1079-L1081
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks
---

# Signature

`async fn fetch_jmap_tasks(&self, account_id: Uuid) -> Result<Vec<ClientTask>>`

# Calls

- [fetch_client_tasks](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks.md)