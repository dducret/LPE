---
type: Rust Method
title: allocate_uid
resource: crates/lpe-imap/src/tests.rs#L179-L185
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/copy_imap_email
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/move_imap_email
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/save_draft_message
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/import_imap_email
---

# Signature

`fn allocate_uid(&self, mailbox_id: Uuid) -> u32`

# Calls

- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [copy_imap_email](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/copy_imap_email.md)
- [move_imap_email](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/move_imap_email.md)
- [save_draft_message](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/save_draft_message.md)
- [import_imap_email](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/import_imap_email.md)