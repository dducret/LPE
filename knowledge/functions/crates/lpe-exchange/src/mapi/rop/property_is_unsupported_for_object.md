---
type: Rust Function
title: property_is_unsupported_for_object
resource: crates/lpe-exchange/src/mapi/rop.rs#L836-L856
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  - functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/flagged_property_error_code
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
---

# Signature

`pub(in crate::mapi) fn property_is_unsupported_for_object( object: Option<&MapiObject>, principal: &AccountPrincipal, tag: u32, ) -> bool`

# Calls

- [get_properties_specific_value_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag.md)
- [property_type](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)
- [logon_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value.md)

# Called by

- [unsupported_specific_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags.md)
- [flagged_property_error_code](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/flagged_property_error_code.md)
- [log_get_properties_specific_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)