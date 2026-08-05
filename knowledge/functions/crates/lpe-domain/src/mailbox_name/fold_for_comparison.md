---
type: Rust Function
title: fold_for_comparison
resource: crates/lpe-domain/src/mailbox_name.rs#L431-L442
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name
  - functions/crates/lpe-domain/src/mailbox_name/fold_list_pattern_text
---

# Signature

`fn fold_for_comparison(value: &str) -> String`

# Called by

- [for_display_name](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name.md)
- [fold_list_pattern_text](../../../../../functions/crates/lpe-domain/src/mailbox_name/fold_list_pattern_text.md)