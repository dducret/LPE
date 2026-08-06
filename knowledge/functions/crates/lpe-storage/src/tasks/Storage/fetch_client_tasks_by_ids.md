---
type: Rust Method
title: fetch_client_tasks_by_ids
resource: crates/lpe-storage/src/tasks.rs#L1361-L1449
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/get_client_task
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_tasks_by_ids
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task
  - functions/crates/lpe-storage/src/tasks/Storage/update_accessible_task_reminder
  - functions/crates/lpe-storage/src/tasks/Storage/delete_client_task
---

# Signature

`pub async fn fetch_client_tasks_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<ClientTask>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)

# Called by

- [get_client_task](../../../../../../functions/crates/lpe-admin-api/src/workspace/get_client_task.md)
- [fetch_jmap_tasks_by_ids](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_tasks_by_ids.md)
- [upsert_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)
- [update_accessible_task_reminder](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_accessible_task_reminder.md)
- [delete_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_client_task.md)