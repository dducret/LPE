---
type: Rust Function
title: push_bcc_recipients
resource: crates/lpe-storage/src/submission/types.rs#L331-L350
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/submission/types/normalize_bcc_recipients
---

# Signature

`fn push_bcc_recipients( output: &mut Vec<SubmittedRecipientInput>, input: &[SubmittedRecipientInput], )`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [normalize_bcc_recipients](../../../../../../functions/crates/lpe-storage/src/submission/types/normalize_bcc_recipients.md)