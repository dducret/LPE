---
type: Rust Function
title: empty_stream_bytes_for_property_tag
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L327-L334
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_tag_type
  - functions/crates/lpe-exchange/src/mapi/properties/streams/string8z_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
---

# Signature

`fn empty_stream_bytes_for_property_tag(property_tag: u32) -> Option<Vec<u8>>`

# Calls

- [property_tag_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_tag_type.md)
- [string8z_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/string8z_bytes.md)

# Called by

- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)