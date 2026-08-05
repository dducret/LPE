---
type: Rust Method
title: destroy_jmap_mailbox
resource: crates/lpe-storage/src/mailboxes.rs#L1252-L1352
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mailboxes/is_system_mailbox_role
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn destroy_jmap_mailbox( &self, account_id: Uuid, mailbox_id: Uuid, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [is_system_mailbox_role](../../../../../../functions/crates/lpe-storage/src/mailboxes/is_system_mailbox_role.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)