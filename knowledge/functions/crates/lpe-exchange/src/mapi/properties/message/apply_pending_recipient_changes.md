---
type: Rust Function
title: apply_pending_recipient_changes
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L669-L691
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
---

# Signature

`pub(in crate::mapi) fn apply_pending_recipient_changes( recipients: &mut Vec<PendingRecipient>, changes: Vec<PendingRecipientChange>, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_modify_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)