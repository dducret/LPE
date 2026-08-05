---
type: Rust Function
title: mailbox_matches_pattern
resource: crates/lpe-imap/src/mailboxes.rs#L729-L737
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/mailbox_pattern_matches
  - functions/crates/lpe-imap/src/mailboxes/special_mailbox_aliases
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub
---

# Signature

`fn mailbox_matches_pattern(mailbox: &JmapMailbox, mailbox_path: &str, pattern: &str) -> bool`

# Calls

- [mailbox_pattern_matches](../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_pattern_matches.md)
- [special_mailbox_aliases](../../../../../functions/crates/lpe-imap/src/mailboxes/special_mailbox_aliases.md)

# Called by

- [handle_mailbox_listing](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing.md)
- [handle_lsub](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub.md)