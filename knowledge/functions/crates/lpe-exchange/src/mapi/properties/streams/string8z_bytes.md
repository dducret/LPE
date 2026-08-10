---
type: Rust Function
title: string8z_bytes
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L530-L537
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/mapi_value_stream_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/streams/empty_stream_bytes_for_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
---

# Signature

`pub(in crate::mapi) fn string8z_bytes(value: &str) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_value_stream_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/mapi_value_stream_bytes.md)
- [empty_stream_bytes_for_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/empty_stream_bytes_for_property_tag.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)