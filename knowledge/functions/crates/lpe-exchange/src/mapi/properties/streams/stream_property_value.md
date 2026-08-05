---
type: Rust Function
title: stream_property_value
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L801-L818
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/decode_string8_stream_value
  - functions/crates/lpe-exchange/src/mapi/properties/streams/decode_utf16_stream_value
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_tag_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target
---

# Signature

`pub(in crate::mapi) fn stream_property_value( property_tag: u32, data: Vec<u8>, ) -> Option<MapiValue>`

# Calls

- [decode_string8_stream_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/decode_string8_stream_value.md)
- [decode_utf16_stream_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/decode_utf16_stream_value.md)
- [property_tag_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_tag_type.md)

# Called by

- [sync_stream_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target.md)