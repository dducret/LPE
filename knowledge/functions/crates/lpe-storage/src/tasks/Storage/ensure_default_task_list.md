---
type: Rust Method
title: ensure_default_task_list
resource: crates/lpe-storage/src/tasks.rs#L1405-L1442
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids
  - functions/crates/lpe-storage/src/tasks/Storage/create_task_list
  - functions/crates/lpe-storage/src/tasks/Storage/update_task_list
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_dav_tasks
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_dav_tasks_by_ids
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids
---

# Signature

`pub(crate) async fn ensure_default_task_list( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, ) -> Result<ClientTaskListRow>`

# Called by

- [upsert_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [upsert_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)
- [fetch_task_lists](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists.md)
- [fetch_task_lists_by_ids](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids.md)
- [create_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/create_task_list.md)
- [update_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_task_list.md)
- [fetch_dav_tasks](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_dav_tasks.md)
- [fetch_dav_tasks_by_ids](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_dav_tasks_by_ids.md)
- [fetch_client_tasks](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks.md)
- [fetch_client_tasks_by_ids](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids.md)