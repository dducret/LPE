---
type: Rust Function
title: mailbox_with_parent
resource: crates/lpe-imap/src/tests.rs#L3872-L3891
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox
  - functions/crates/lpe-imap/src/tests/mailbox
---

# Signature

`fn mailbox_with_parent( id: &str, parent_id: Option<Uuid>, role: &str, name: &str, sort_order: i32, ) -> JmapMailbox`

# Called by

- [create_imap_mailbox](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox.md)
- [rename_imap_mailbox](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox.md)
- [mailbox](../../../../../functions/crates/lpe-imap/src/tests/mailbox.md)