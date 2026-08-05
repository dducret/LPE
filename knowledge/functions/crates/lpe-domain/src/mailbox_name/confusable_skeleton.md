---
type: Rust Function
title: confusable_skeleton
resource: crates/lpe-domain/src/mailbox_name.rs#L417-L429
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/skeleton
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name
---

# Signature

`fn confusable_skeleton(value: &str) -> String`

# Calls

- [skeleton](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/skeleton.md)

# Called by

- [for_display_name](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name.md)