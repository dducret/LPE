---
type: Rust Function
title: mailbox_pattern_matches
resource: crates/lpe-imap/src/mailboxes.rs#L725-L727
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/list_pattern_matches
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern
---

# Signature

`fn mailbox_pattern_matches(name: &str, pattern: &str) -> bool`

# Calls

- [list_pattern_matches](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/list_pattern_matches.md)

# Called by

- [mailbox_matches_pattern](../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern.md)