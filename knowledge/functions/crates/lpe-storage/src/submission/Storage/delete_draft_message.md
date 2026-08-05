---
type: Rust Method
title: delete_draft_message
resource: crates/lpe-storage/src/submission.rs#L1149-L1163
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/submission/Storage/delete_draft_message_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn delete_draft_message( &self, account_id: Uuid, message_id: Uuid, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [delete_draft_message_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/delete_draft_message_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)