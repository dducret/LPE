---
type: Rust Function
title: normalize_visible_recipients
resource: crates/lpe-storage/src/submission/types.rs#L291-L298
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/submission/types/push_recipients
  called_by:
  - functions/crates/lpe-storage/src/shared/visible_recipients_exclude_bcc
  - functions/crates/lpe-storage/src/shared/participants_normalized_ignores_bcc_addresses
  - functions/crates/lpe-storage/src/shared/participants_normalized_remains_visible_only_even_with_bcc_display_name
  - functions/crates/lpe-storage/src/shared/participants_normalized_allows_null_reverse_path
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) fn normalize_visible_recipients( input: &SubmitMessageInput, ) -> Vec<(&'static str, SubmittedRecipientInput)>`

# Calls

- [push_recipients](../../../../../../functions/crates/lpe-storage/src/submission/types/push_recipients.md)

# Called by

- [visible_recipients_exclude_bcc](../../../../../../functions/crates/lpe-storage/src/shared/visible_recipients_exclude_bcc.md)
- [participants_normalized_ignores_bcc_addresses](../../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_ignores_bcc_addresses.md)
- [participants_normalized_remains_visible_only_even_with_bcc_display_name](../../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_remains_visible_only_even_with_bcc_display_name.md)
- [participants_normalized_allows_null_reverse_path](../../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_allows_null_reverse_path.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)