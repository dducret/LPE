---
type: Rust Function
title: mapi_value_stream_bytes
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L309-L324
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/streams/utf16_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_tag_type
  - functions/crates/lpe-exchange/src/mapi/properties/streams/string8z_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
---

# Signature

`fn mapi_value_stream_bytes(property_tag: u32, value: MapiValue) -> Option<Vec<u8>>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [utf16_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/utf16_bytes.md)
- [property_tag_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_tag_type.md)
- [string8z_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/string8z_bytes.md)

# Called by

- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)