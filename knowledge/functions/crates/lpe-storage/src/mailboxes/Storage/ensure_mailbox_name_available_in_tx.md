---
type: Rust Method
title: ensure_mailbox_name_available_in_tx
resource: crates/lpe-storage/src/mailboxes.rs#L264-L300
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with
  called_by:
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox
---

# Signature

`pub(crate) async fn ensure_mailbox_name_available_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, parent_id: Option<Uuid>, display_name: &str, except_mailbox_id: Option<Uuid>, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [collides_with](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with.md)

# Called by

- [create_mailbox](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_mailbox.md)
- [insert_imap_custom_mailbox_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx.md)
- [create_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [update_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox.md)
- [rename_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)