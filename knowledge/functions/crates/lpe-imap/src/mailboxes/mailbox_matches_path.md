---
type: Rust Function
title: mailbox_matches_path
resource: crates/lpe-imap/src/mailboxes.rs#L685-L695
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/mailbox_name_matches
  - functions/crates/lpe-imap/src/mailboxes/render_mailbox_path
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_status
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path
---

# Signature

`fn mailbox_matches_path( mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], path: &lpe_domain::MailboxPath, ) -> bool`

# Calls

- [mailbox_name_matches](../../../../../functions/crates/lpe-imap/src/render/mailbox_name_matches.md)
- [render_mailbox_path](../../../../../functions/crates/lpe-imap/src/mailboxes/render_mailbox_path.md)

# Called by

- [handle_status](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_status.md)
- [handle_select_mode](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode.md)
- [resolve_mailbox_path](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path.md)