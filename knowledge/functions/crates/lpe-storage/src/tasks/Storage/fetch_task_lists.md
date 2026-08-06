---
type: Rust Method
title: fetch_task_lists
resource: crates/lpe-storage/src/tasks.rs#L655-L711
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/list_client_task_lists
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_task_lists
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_task_collections
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task
---

# Signature

`pub async fn fetch_task_lists(&self, account_id: Uuid) -> Result<Vec<ClientTaskList>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)

# Called by

- [list_client_task_lists](../../../../../../functions/crates/lpe-admin-api/src/workspace/list_client_task_lists.md)
- [fetch_jmap_task_lists](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_task_lists.md)
- [fetch_accessible_task_collections](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_task_collections.md)
- [upsert_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)