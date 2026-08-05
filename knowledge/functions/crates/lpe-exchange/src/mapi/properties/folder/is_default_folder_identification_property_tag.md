---
type: Rust Function
title: is_default_folder_identification_property_tag
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L136-L144
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_response_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/set_property_debug_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_value_for_debug
  - functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values
---

# Signature

`pub(in crate::mapi) fn is_default_folder_identification_property_tag(property_tag: u32) -> bool`

# Calls

- [is_scalar_default_folder_entry_id_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag.md)

# Called by

- [strips_default_folder_identification_value_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value_for_folder_id.md)
- [default_folder_getprops_response_values_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_response_values_for_debug.md)
- [set_property_debug_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/set_property_debug_name.md)
- [get_properties_specific_value_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_value_for_debug.md)
- [apply_mapi_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values.md)