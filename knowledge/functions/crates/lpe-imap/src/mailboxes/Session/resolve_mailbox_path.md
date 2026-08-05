---
type: Rust Method
title: resolve_mailbox_path
resource: crates/lpe-imap/src/mailboxes.rs#L668-L682
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_rename
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_name
---

# Signature

`async fn resolve_mailbox_path( &self, mailbox_path: &lpe_domain::MailboxPath, ) -> Result<JmapMailbox>`

# Calls

- [mailbox_matches_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path.md)

# Called by

- [handle_rename](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_rename.md)
- [resolve_mailbox_by_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)
- [resolve_mailbox_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_name.md)