---
type: Rust Method
title: fetch_task_list_grant
resource: crates/lpe-storage/src/tasks.rs#L563-L610
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  called_by:
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant
---

# Signature

`pub async fn fetch_task_list_grant( &self, owner_account_id: Uuid, task_list_id: Uuid, grantee_account_id: Uuid, ) -> Result<Option<TaskListGrant>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)

# Called by

- [upsert_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)