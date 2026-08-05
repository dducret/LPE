---
type: Rust Method
title: upsert_task_list_grant
resource: crates/lpe-storage/src/tasks.rs#L368-L469
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx
  - functions/crates/lpe-storage/src/tasks/Storage/load_task_list_in_tx
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_by_email_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_task_access_change
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_list_grant
---

# Signature

`pub async fn upsert_task_list_grant( &self, input: TaskListGrantInput, audit: AuditEntryInput, ) -> Result<TaskListGrant>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [validate_collaboration_rights](../../../../../../functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights.md)
- [load_account_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx.md)
- [load_task_list_in_tx](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/load_task_list_in_tx.md)
- [load_account_identity_by_email_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_by_email_in_tx.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_task_access_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_task_access_change.md)
- [fetch_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_list_grant.md)