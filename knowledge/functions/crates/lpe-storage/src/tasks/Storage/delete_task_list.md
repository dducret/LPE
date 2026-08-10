---
type: Rust Method
title: delete_task_list
resource: crates/lpe-storage/src/tasks.rs#L925-L998
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/load_task_list_in_tx
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/change/Storage/emit_task_access_change
  called_by:
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_task_list
---

# Signature

`pub async fn delete_task_list(&self, account_id: Uuid, task_list_id: Uuid) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [load_task_list_in_tx](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/load_task_list_in_tx.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [insert_collaboration_tombstone_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [emit_task_access_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_task_access_change.md)

# Called by

- [delete_jmap_task_list](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_task_list.md)