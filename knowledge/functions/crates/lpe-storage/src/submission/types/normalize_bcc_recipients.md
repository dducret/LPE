---
type: Rust Function
title: normalize_bcc_recipients
resource: crates/lpe-storage/src/submission/types.rs#L300-L304
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/submission/types/push_bcc_recipients
  called_by:
  - functions/crates/lpe-storage/src/shared/bcc_recipients_are_kept_separately
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) fn normalize_bcc_recipients(input: &SubmitMessageInput) -> Vec<SubmittedRecipientInput>`

# Calls

- [push_bcc_recipients](../../../../../../functions/crates/lpe-storage/src/submission/types/push_bcc_recipients.md)

# Called by

- [bcc_recipients_are_kept_separately](../../../../../../functions/crates/lpe-storage/src/shared/bcc_recipients_are_kept_separately.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)