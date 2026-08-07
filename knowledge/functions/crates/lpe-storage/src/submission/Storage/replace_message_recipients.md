---
type: Rust Method
title: replace_message_recipients
resource: crates/lpe-storage/src/submission.rs#L116-L243
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/submission/insert_visible_recipient
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn replace_message_recipients( &self, account_id: Uuid, message_id: Uuid, to: &[SubmittedRecipientInput], cc: &[SubmittedRecipientInput], bcc: &[SubmittedRecipientInput], audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_visible_recipient](../../../../../../functions/crates/lpe-storage/src/submission/insert_visible_recipient.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [rotate_active_mapi_message_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)