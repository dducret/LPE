---
type: Rust Method
title: list_pattern_matches
resource: crates/lpe-domain/src/mailbox_name.rs#L195-L204
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/fold_list_pattern_text
  - functions/crates/lpe-domain/src/mailbox_name/list_pattern_match_from
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/mailbox_pattern_matches
---

# Signature

`pub fn list_pattern_matches(name: &str, pattern: &str) -> bool`

# Calls

- [fold_list_pattern_text](../../../../../../functions/crates/lpe-domain/src/mailbox_name/fold_list_pattern_text.md)
- [list_pattern_match_from](../../../../../../functions/crates/lpe-domain/src/mailbox_name/list_pattern_match_from.md)

# Called by

- [mailbox_pattern_matches](../../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_pattern_matches.md)