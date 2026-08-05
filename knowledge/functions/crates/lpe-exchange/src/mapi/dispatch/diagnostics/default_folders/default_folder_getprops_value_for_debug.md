---
type: Rust Function
title: default_folder_getprops_value_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L262-L283
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_response_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_value_for_debug
---

# Signature

`pub(in crate::mapi::dispatch) fn default_folder_getprops_value_for_debug( tag: u32, value: &MapiValue, ) -> String`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [default_folder_entry_id_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug.md)

# Called by

- [default_folder_getprops_response_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_response_values_for_debug.md)
- [get_properties_specific_value_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_value_for_debug.md)