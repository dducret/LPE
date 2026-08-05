---
type: Rust Function
title: get_properties_specific_value_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L716-L744
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_value_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_response_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_view_response_values_for_debug
---

# Signature

`fn get_properties_specific_value_for_debug(tag: u32, value: &MapiValue) -> String`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [is_default_folder_identification_property_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag.md)
- [default_folder_getprops_value_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_value_for_debug.md)

# Called by

- [get_properties_specific_response_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_response_values_for_debug.md)
- [get_properties_view_response_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_view_response_values_for_debug.md)