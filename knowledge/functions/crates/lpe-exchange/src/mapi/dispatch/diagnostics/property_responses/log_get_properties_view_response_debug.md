---
type: Rust Function
title: log_get_properties_view_response_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L633-L668
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_view_response_values_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_get_properties_view_response_debug( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, property_response: &[u8], )`

# Calls

- [property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [get_properties_view_response_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_view_response_values_for_debug.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)