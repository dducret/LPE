---
type: Rust Function
title: log_get_properties_default_folder_response_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L139-L186
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_response_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_get_properties_default_folder_response_debug( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, property_response: &[u8], )`

# Calls

- [property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [input_handle_index](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [summarize_get_properties_probe_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response.md)
- [default_folder_getprops_response_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_response_values_for_debug.md)
- [default_folder_hierarchy_projection_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)