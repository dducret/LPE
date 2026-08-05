---
type: Rust Method
title: load_task_list_in_tx
resource: crates/lpe-storage/src/tasks.rs#L1444-L1479
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant
  - functions/crates/lpe-storage/src/tasks/Storage/delete_task_list
---

# Signature

`pub(crate) async fn load_task_list_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, task_list_id: Uuid, ) -> Result<ClientTaskListRow>`

# Called by

- [upsert_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)
- [delete_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list.md)