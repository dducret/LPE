---
type: Rust Function
title: wlink_guid_property_value
resource: crates/lpe-exchange/src/mapi/properties.rs#L1022-L1027
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
---

# Signature

`fn wlink_guid_property_value(property_tag: u32, guid: [u8; 16]) -> MapiValue`

# Calls

- [property_type](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)

# Called by

- [common_view_named_view_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)