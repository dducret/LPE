---
type: Rust Function
title: mailbox_name_match
resource: crates/lpe-imap/src/tests.rs#L3922-L3933
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox
---

# Signature

`fn mailbox_name_match( mailboxes: &[JmapMailbox], requested_name: &str, parent_id: Option<Uuid>, ) -> Option<Uuid>`

# Called by

- [create_imap_mailbox](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox.md)
- [rename_imap_mailbox](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox.md)