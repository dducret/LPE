---
type: Rust Function
title: should_log_outlook_surface_getprops_info
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L3-L24
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/format_outlook_surface_folder_getprops_trace
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
---

# Signature

`pub(in crate::mapi::dispatch) fn should_log_outlook_surface_getprops_info( object: Option<&MapiObject>, ) -> bool`

# Called by

- [format_outlook_surface_folder_getprops_trace](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/format_outlook_surface_folder_getprops_trace.md)
- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)