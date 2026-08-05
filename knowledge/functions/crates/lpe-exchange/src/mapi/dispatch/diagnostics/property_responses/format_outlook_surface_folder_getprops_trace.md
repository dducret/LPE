---
type: Rust Function
title: format_outlook_surface_folder_getprops_trace
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L26-L52
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/should_log_outlook_surface_getprops_info
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/calendar_folder_getprops_trace_summarizes_response_contract
---

# Signature

`pub(in crate::mapi::dispatch) fn format_outlook_surface_folder_getprops_trace( request_id: &str, request: &RopRequest, object: Option<&MapiObject>, property_response: &[u8], ) -> Option<String>`

# Calls

- [should_log_outlook_surface_getprops_info](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/should_log_outlook_surface_getprops_info.md)
- [property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [getprops_contract_response_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [calendar_folder_getprops_trace_summarizes_response_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/calendar_folder_getprops_trace_summarizes_response_contract.md)