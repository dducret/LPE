---
type: Rust Function
title: pending_body_text_property
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L880-L888
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/plain_text_from_html_body
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/message/meeting_request_attachment
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_message_size
---

# Signature

`pub(super) fn pending_body_text_property(properties: &HashMap<u32, MapiValue>) -> String`

# Calls

- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property.md)
- [plain_text_from_html_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/plain_text_from_html_body.md)

# Called by

- [jmap_import_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message.md)
- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)
- [meeting_request_attachment](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/meeting_request_attachment.md)
- [pending_message_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_message_size.md)