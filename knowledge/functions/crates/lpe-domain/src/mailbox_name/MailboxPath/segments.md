---
type: Rust Method
title: segments
resource: crates/lpe-domain/src/mailbox_name.rs#L157-L159
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox
---

# Signature

`pub fn segments(&self) -> &[MailboxSegment]`

# Called by

- [create_imap_mailbox](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox.md)
- [rename_imap_mailbox](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/rename_imap_mailbox.md)
- [create_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)
- [rename_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)