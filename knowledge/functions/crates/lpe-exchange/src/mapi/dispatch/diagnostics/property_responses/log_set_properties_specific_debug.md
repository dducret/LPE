---
type: Rust Function
title: log_set_properties_specific_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L62-L115
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_values_stripped_by_safe_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/folder_profile_property_storage_mode_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_set_properties_specific_debug( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, probe: &SetPropertiesProbeRequest, )`

# Calls

- [default_folder_identification_values_stripped_by_safe_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_values_stripped_by_safe_values.md)
- [folder_profile_property_storage_mode_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/folder_profile_property_storage_mode_for_debug.md)

# Called by

- [append_set_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)