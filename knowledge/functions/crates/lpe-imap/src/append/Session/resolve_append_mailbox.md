---
type: Rust Method
title: resolve_append_mailbox
resource: crates/lpe-imap/src/append.rs#L233-L235
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_name
  called_by:
  - functions/crates/lpe-imap/src/append/Session/handle_append
---

# Signature

`async fn resolve_append_mailbox(&self, mailbox_name: &str) -> Result<JmapMailbox>`

# Calls

- [resolve_mailbox_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_name.md)

# Called by

- [handle_append](../../../../../../functions/crates/lpe-imap/src/append/Session/handle_append.md)