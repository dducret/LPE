---
type: Rust Function
title: post_hierarchy_setprops_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L98-L136
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_values_stripped_by_safe_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/set_properties_problem_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/set_properties_problem_details_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(in crate::mapi::dispatch) fn post_hierarchy_setprops_contract( request: &RopRequest, object: Option<&MapiObject>, probe: &SetPropertiesProbeRequest, response: &[u8], ) -> String`

# Calls

- [default_folder_identification_values_stripped_by_safe_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_values_stripped_by_safe_values.md)
- [read_response_error_code](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)
- [set_properties_problem_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/set_properties_problem_count.md)
- [set_properties_problem_details_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/set_properties_problem_details_for_debug.md)

# Called by

- [append_set_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)