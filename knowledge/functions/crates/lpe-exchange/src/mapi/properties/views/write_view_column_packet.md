---
type: Rust Function
title: write_view_column_packet
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L644-L690
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_tag_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
---

# Signature

`fn write_view_column_packet( value: &mut Vec<u8>, property_tag: u32, width: u32, flags: u32, kind: ViewColumnKind, )`

# Calls

- [property_tag_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_tag_type.md)

# Called by

- [view_descriptor_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)