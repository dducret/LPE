---
type: Rust Function
title: mailbox_name_collides
resource: crates/lpe-imap/src/tests.rs#L3908-L3920
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox
---

# Signature

`fn mailbox_name_collides( mailboxes: &[JmapMailbox], requested_name: &str, parent_id: Option<Uuid>, except_mailbox_id: Option<Uuid>, ) -> bool`

# Calls

- [collides_with](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with.md)

# Called by

- [create_imap_mailbox](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox.md)
- [rename_imap_mailbox](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox.md)