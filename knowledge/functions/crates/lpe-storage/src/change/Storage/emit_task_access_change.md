---
type: Rust Method
title: emit_task_access_change
resource: crates/lpe-storage/src/change.rs#L410-L450
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
  called_by:
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant
  - functions/crates/lpe-storage/src/tasks/Storage/delete_task_list_grant
  - functions/crates/lpe-storage/src/tasks/Storage/create_task_list
  - functions/crates/lpe-storage/src/tasks/Storage/update_task_list
  - functions/crates/lpe-storage/src/tasks/Storage/delete_task_list
  - functions/crates/lpe-storage/src/tasks/Storage/delete_client_task
---

# Signature

`pub(crate) async fn emit_task_access_change( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, task_list_ids: &[Uuid], extra_principal_account_ids: &[Uuid], ) -> Result<()>`

# Calls

- [emit_canonical_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)

# Called by

- [upsert_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)
- [upsert_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)
- [delete_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list_grant.md)
- [create_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/create_task_list.md)
- [update_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_task_list.md)
- [delete_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list.md)
- [delete_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_client_task.md)