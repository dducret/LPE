---
type: Rust Function
title: log_get_properties_specific_response_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L188-L285
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_response_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/associated_config_debug_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_get_properties_specific_response_debug( principal: &AccountPrincipal, session: &mut MapiSession, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, property_response: &[u8], )`

# Calls

- [property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [input_handle_index](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [summarize_get_properties_probe_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response.md)
- [get_properties_specific_response_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_response_values_for_debug.md)
- [associated_config_debug_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/associated_config_debug_identity.md)
- [record_outlook_umolk_getprops_materialization](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization.md)
- [property_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)