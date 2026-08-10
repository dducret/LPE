---
type: Rust Function
title: mapi_submit_from_email
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L765-L792
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/submitted_recipients_from_addresses
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/mapi_submit_from_existing_email
---

# Signature

`pub(in crate::mapi) fn mapi_submit_from_email( principal: &AccountPrincipal, email: &JmapEmail, attachments: Vec<AttachmentUploadInput>, ) -> SubmitMessageInput`

# Calls

- [submitted_recipients_from_addresses](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/submitted_recipients_from_addresses.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [mapi_submit_from_existing_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/mapi_submit_from_existing_email.md)