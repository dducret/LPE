---
type: Rust Method
title: resolve_mailbox_name
resource: crates/lpe-imap/src/mailboxes.rs#L663-L666
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path
  called_by:
  - functions/crates/lpe-imap/src/append/Session/resolve_append_mailbox
---

# Signature

`pub(crate) async fn resolve_mailbox_name(&self, mailbox_name: &str) -> Result<JmapMailbox>`

# Calls

- [parse_mailbox_path](../../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path.md)
- [resolve_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path.md)

# Called by

- [resolve_append_mailbox](../../../../../../functions/crates/lpe-imap/src/append/Session/resolve_append_mailbox.md)