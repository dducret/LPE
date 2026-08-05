---
type: Rust Function
title: system_mailbox_for_name
resource: crates/lpe-imap/src/tests.rs#L3893-L3906
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox
---

# Signature

`fn system_mailbox_for_name(name: &str) -> Option<(&'static str, &'static str, i32)>`

# Called by

- [create_imap_mailbox](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/create_imap_mailbox.md)