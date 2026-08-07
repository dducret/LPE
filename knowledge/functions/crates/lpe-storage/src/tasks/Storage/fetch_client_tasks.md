---
type: Rust Method
title: fetch_client_tasks
resource: crates/lpe-storage/src/tasks.rs#L1281-L1359
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/list_client_tasks
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_tasks
  - functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace
---

# Signature

`pub async fn fetch_client_tasks(&self, account_id: Uuid) -> Result<Vec<ClientTask>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)

# Called by

- [list_client_tasks](../../../../../../functions/crates/lpe-admin-api/src/workspace/list_client_tasks.md)
- [fetch_jmap_tasks](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_tasks.md)
- [fetch_client_workspace](../../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace.md)