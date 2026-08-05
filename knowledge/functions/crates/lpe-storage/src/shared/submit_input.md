---
type: Rust Function
title: submit_input
resource: crates/lpe-storage/src/shared.rs#L852-L884
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/shared/visible_recipients_exclude_bcc
  - functions/crates/lpe-storage/src/shared/bcc_recipients_are_kept_separately
  - functions/crates/lpe-storage/src/shared/participants_normalized_ignores_bcc_addresses
  - functions/crates/lpe-storage/src/shared/participants_normalized_remains_visible_only_even_with_bcc_display_name
  - functions/crates/lpe-storage/src/shared/participants_normalized_allows_null_reverse_path
---

# Signature

`fn submit_input() -> SubmitMessageInput`

# Called by

- [visible_recipients_exclude_bcc](../../../../../functions/crates/lpe-storage/src/shared/visible_recipients_exclude_bcc.md)
- [bcc_recipients_are_kept_separately](../../../../../functions/crates/lpe-storage/src/shared/bcc_recipients_are_kept_separately.md)
- [participants_normalized_ignores_bcc_addresses](../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_ignores_bcc_addresses.md)
- [participants_normalized_remains_visible_only_even_with_bcc_display_name](../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_remains_visible_only_even_with_bcc_display_name.md)
- [participants_normalized_allows_null_reverse_path](../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_allows_null_reverse_path.md)