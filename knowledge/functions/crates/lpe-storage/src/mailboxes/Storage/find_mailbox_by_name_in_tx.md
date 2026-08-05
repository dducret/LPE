---
type: Rust Method
title: find_mailbox_by_name_in_tx
resource: crates/lpe-storage/src/mailboxes.rs#L302-L336
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with
  called_by:
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox
---

# Signature

`async fn find_mailbox_by_name_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, parent_id: Option<Uuid>, display_name: &str, ) -> Result<Option<Uuid>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [collides_with](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with.md)

# Called by

- [create_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)
- [rename_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)