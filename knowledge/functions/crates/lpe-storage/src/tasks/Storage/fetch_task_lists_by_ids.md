---
type: Rust Method
title: fetch_task_lists_by_ids
resource: crates/lpe-storage/src/tasks.rs#L713-L778
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list
  called_by:
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_task_lists_by_ids
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_dav_task
---

# Signature

`pub async fn fetch_task_lists_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<ClientTaskList>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)

# Called by

- [fetch_jmap_task_lists_by_ids](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_task_lists_by_ids.md)
- [upsert_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)
- [upsert_dav_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_dav_task.md)