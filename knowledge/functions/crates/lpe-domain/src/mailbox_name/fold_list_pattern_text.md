---
type: Rust Function
title: fold_list_pattern_text
resource: crates/lpe-domain/src/mailbox_name.rs#L444-L446
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/fold_for_comparison
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/list_pattern_matches
---

# Signature

`fn fold_list_pattern_text(value: &str) -> String`

# Calls

- [fold_for_comparison](../../../../../functions/crates/lpe-domain/src/mailbox_name/fold_for_comparison.md)

# Called by

- [list_pattern_matches](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/list_pattern_matches.md)