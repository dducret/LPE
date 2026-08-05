---
type: Rust Function
title: get_properties_specific_value_tag
resource: crates/lpe-exchange/src/mapi/rop.rs#L858-L862
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property_row_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  - functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties
---

# Signature

`fn get_properties_specific_value_tag(object: Option<&MapiObject>, tag: u32) -> u32`

# Calls

- [get_properties_specific_typed_value_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag.md)

# Called by

- [serialize_object_property_row_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property_row_with_custom.md)
- [fallback_default_specific_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)
- [property_is_unsupported_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object.md)
- [size_limited_specific_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties.md)