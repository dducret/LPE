---
type: Rust Function
title: mapi_contact_custom_property_values_from_map
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L105-L123
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact
---

# Signature

`pub(super) fn mapi_contact_custom_property_values_from_map( properties: &HashMap<u32, MapiValue>, ) -> Vec<MapiContactCustomPropertyValue>`

# Calls

- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [property_type_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)

# Called by

- [save_pending_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact.md)