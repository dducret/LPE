---
type: Rust Function
title: submitted_recipients_from_pending
resource: crates/lpe-exchange/src/mapi/dispatch/recipients.rs#L3-L25
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_recipient_replacement
---

# Signature

`pub(super) fn submitted_recipients_from_pending( recipients: &[PendingRecipient], ) -> ( Vec<SubmittedRecipientInput>, Vec<SubmittedRecipientInput>, Vec<SubmittedRecipientInput>, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [apply_staged_message_recipient_replacement](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_recipient_replacement.md)