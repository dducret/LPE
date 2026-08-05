---
type: Rust Function
title: special_mailbox_aliases
resource: crates/lpe-imap/src/mailboxes.rs#L761-L777
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern
---

# Signature

`fn special_mailbox_aliases(role: &str) -> &'static [&'static str]`

# Called by

- [mailbox_matches_pattern](../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern.md)