---
type: Rust Function
title: get_properties_specific_typed_value_tag
resource: crates/lpe-exchange/src/mapi/rop.rs#L882-L904
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_candidate_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties
---

# Signature

`fn get_properties_specific_typed_value_tag( object: Option<&MapiObject>, tag: u32, ) -> Option<(u32, u16)>`

# Calls

- [property_type_code](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)
- [get_properties_specific_candidate_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_candidate_tags.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [write_flagged_property_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row.md)
- [get_properties_specific_value_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag.md)
- [size_limited_specific_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties.md)