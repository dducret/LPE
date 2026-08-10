---
type: Rust Function
title: meeting_request_attachment
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L616-L726
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
---

# Signature

`fn meeting_request_attachment( properties: &HashMap<u32, MapiValue>, recipients: &[PendingRecipient], organizer_address: &str, organizer_name: Option<&str>, ) -> Vec<AttachmentUploadInput>`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [pending_body_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)