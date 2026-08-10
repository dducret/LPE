---
type: Rust Function
title: pending_recipients_for_import
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L519-L541
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
---

# Signature

`pub(in crate::mapi) fn pending_recipients_for_import( recipients: &[PendingRecipient], ) -> ( Vec<SubmittedRecipientInput>, Vec<SubmittedRecipientInput>, Vec<SubmittedRecipientInput>, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [jmap_import_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message.md)
- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)