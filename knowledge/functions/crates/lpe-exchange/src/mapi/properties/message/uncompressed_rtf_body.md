---
type: Rust Function
title: uncompressed_rtf_body
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L315-L320
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/append_rtf_escaped_text
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/message/rtf_uncompressed_container
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
---

# Signature

`pub(in crate::mapi) fn uncompressed_rtf_body(body_text: &str) -> Vec<u8>`

# Calls

- [append_rtf_escaped_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/append_rtf_escaped_text.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rtf_uncompressed_container](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/rtf_uncompressed_container.md)

# Called by

- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)