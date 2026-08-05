---
type: Rust Function
title: html_body_from_plain_text
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L294-L313
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
---

# Signature

`pub(in crate::mapi) fn html_body_from_plain_text(body_text: &str) -> Option<String>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)