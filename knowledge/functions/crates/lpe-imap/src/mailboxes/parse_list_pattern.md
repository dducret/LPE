---
type: Rust Function
title: parse_list_pattern
resource: crates/lpe-imap/src/mailboxes.rs#L697-L703
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub
---

# Signature

`fn parse_list_pattern(arguments: &str) -> Result<String>`

# Called by

- [handle_mailbox_listing](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing.md)
- [handle_lsub](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub.md)