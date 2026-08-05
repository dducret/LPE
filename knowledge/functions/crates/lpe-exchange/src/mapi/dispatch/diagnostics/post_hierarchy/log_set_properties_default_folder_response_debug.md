---
type: Rust Function
title: log_set_properties_default_folder_response_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L138-L174
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/set_properties_problem_details_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_set_properties_default_folder_response_debug( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, probe: &SetPropertiesProbeRequest, response: &[u8], )`

# Calls

- [set_properties_problem_details_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/set_properties_problem_details_for_debug.md)

# Called by

- [append_set_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)