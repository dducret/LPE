---
type: Rust Method
title: rename_imap_mailbox
resource: crates/lpe-storage/src/mailboxes.rs#L1035-L1191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mailboxes/is_system_mailbox_role
  - functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments
  - functions/crates/lpe-storage/src/mailboxes/Storage/find_mailbox_by_name_in_tx
  - functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_parent_valid_in_tx
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn rename_imap_mailbox( &self, account_id: Uuid, mailbox_id: Uuid, name: &str, audit: AuditEntryInput, ) -> Result<JmapMailbox>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [is_system_mailbox_role](../../../../../../functions/crates/lpe-storage/src/mailboxes/is_system_mailbox_role.md)
- [segments](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments.md)
- [find_mailbox_by_name_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/find_mailbox_by_name_in_tx.md)
- [insert_imap_custom_mailbox_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [ensure_mailbox_parent_valid_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_parent_valid_in_tx.md)
- [ensure_mailbox_name_available_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)