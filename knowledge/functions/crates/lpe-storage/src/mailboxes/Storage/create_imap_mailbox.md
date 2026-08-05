---
type: Rust Method
title: create_imap_mailbox
resource: crates/lpe-storage/src/mailboxes.rs#L804-L910
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-storage/src/util/system_mailbox_role_for_display_name
  - functions/crates/lpe-storage/src/util/canonical_system_mailbox_display_name
  - functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments
  - functions/crates/lpe-storage/src/mailboxes/Storage/find_mailbox_by_name_in_tx
  - functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn create_imap_mailbox( &self, account_id: Uuid, name: &str, audit: AuditEntryInput, ) -> Result<JmapMailbox>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [system_mailbox_role_for_display_name](../../../../../../functions/crates/lpe-storage/src/util/system_mailbox_role_for_display_name.md)
- [canonical_system_mailbox_display_name](../../../../../../functions/crates/lpe-storage/src/util/canonical_system_mailbox_display_name.md)
- [ensure_mailbox](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [segments](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments.md)
- [find_mailbox_by_name_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/find_mailbox_by_name_in_tx.md)
- [insert_imap_custom_mailbox_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)