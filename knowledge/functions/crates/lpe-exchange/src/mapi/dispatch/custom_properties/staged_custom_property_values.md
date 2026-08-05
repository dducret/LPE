---
type: Rust Function
title: staged_custom_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L413-L439
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request
---

# Signature

`fn staged_custom_property_values( object: Option<&MapiObject>, property_tags: Option<&[u32]>, ) -> Vec<MapiCustomPropertyValue>`

# Calls

- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [property_type_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)

# Called by

- [copy_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_custom_property_values_for_request.md)
- [copy_all_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request.md)