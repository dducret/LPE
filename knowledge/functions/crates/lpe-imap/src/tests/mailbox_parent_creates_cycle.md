---
type: Rust Function
title: mailbox_parent_creates_cycle
resource: crates/lpe-imap/src/tests.rs#L3935-L3951
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox
---

# Signature

`fn mailbox_parent_creates_cycle( mailboxes: &[JmapMailbox], mailbox_id: Uuid, parent_id: Option<Uuid>, ) -> bool`

# Called by

- [rename_imap_mailbox](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox.md)