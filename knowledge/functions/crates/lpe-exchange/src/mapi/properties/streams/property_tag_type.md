---
type: Rust Function
title: property_tag_type
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L335-L337
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/mapi_value_stream_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/streams/empty_stream_bytes_for_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/streams/stream_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/views/write_view_column_packet
---

# Signature

`pub(super) fn property_tag_type(property_tag: u32) -> u32`

# Called by

- [mapi_value_stream_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/mapi_value_stream_bytes.md)
- [empty_stream_bytes_for_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/empty_stream_bytes_for_property_tag.md)
- [stream_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/stream_property_value.md)
- [write_view_column_packet](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/write_view_column_packet.md)