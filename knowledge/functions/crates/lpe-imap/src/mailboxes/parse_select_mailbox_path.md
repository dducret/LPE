---
type: Rust Function
title: parse_select_mailbox_path
resource: crates/lpe-imap/src/mailboxes.rs#L705-L723
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode
---

# Signature

`fn parse_select_mailbox_path( arguments: &str, command_name: &str, ) -> Result<lpe_domain::MailboxPath>`

# Calls

- [parse_mailbox_path](../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path.md)

# Called by

- [handle_select_mode](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode.md)