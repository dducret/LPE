---
type: Rust Method
title: fetch_dav_tasks
resource: crates/lpe-storage/src/tasks.rs#L1072-L1150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list
---

# Signature

`pub async fn fetch_dav_tasks(&self, account_id: Uuid) -> Result<Vec<DavTask>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)