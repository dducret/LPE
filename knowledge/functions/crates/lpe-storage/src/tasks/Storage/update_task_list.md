---
type: Rust Method
title: update_task_list
resource: crates/lpe-storage/src/tasks.rs#L842-L923
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_task_access_change
  - functions/crates/lpe-storage/src/tasks/types/map_task_list
  called_by:
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_jmap_task_list
---

# Signature

`pub async fn update_task_list(&self, input: UpdateTaskListInput) -> Result<ClientTaskList>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_task_access_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_task_access_change.md)
- [map_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/types/map_task_list.md)

# Called by

- [update_jmap_task_list](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_jmap_task_list.md)