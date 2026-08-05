---
type: Rust Method
title: upsert_collaboration_grant
resource: crates/lpe-storage/src/collaboration/grants.rs#L16-L232
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx
  - functions/crates/lpe-storage/src/submission/Storage/load_account_identity_by_email_in_tx
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_contact_book_in_tx
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx
  - functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_grant_change
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_collaboration_grant
---

# Signature

`pub async fn upsert_collaboration_grant( &self, input: CollaborationGrantInput, audit: AuditEntryInput, ) -> Result<CollaborationGrant>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [validate_collaboration_rights](../../../../../../../functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights.md)
- [load_account_identity_in_tx](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_in_tx.md)
- [load_account_identity_by_email_in_tx](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/load_account_identity_by_email_in_tx.md)
- [ensure_default_contact_book_in_tx](../../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_contact_book_in_tx.md)
- [ensure_default_calendar_in_tx](../../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx.md)
- [ensure_default_task_list](../../../../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)
- [allocate_account_modseq_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [insert_audit](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_collaboration_grant_change](../../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_grant_change.md)
- [fetch_outgoing_collaboration_grants](../../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants.md)
- [fetch_collaboration_grant](../../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_collaboration_grant.md)