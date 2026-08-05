---
type: Rust Function
title: push_recipients
resource: crates/lpe-storage/src/submission/types.rs#L306-L329
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
  - functions/crates/lpe-storage/src/submission/types/normalize_visible_recipients
---

# Signature

`pub(crate) fn push_recipients( output: &mut Vec<(&'static str, SubmittedRecipientInput)>, kind: &'static str, input: &[SubmittedRecipientInput], )`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [normalize_visible_recipients](../../../../../../functions/crates/lpe-storage/src/submission/types/normalize_visible_recipients.md)