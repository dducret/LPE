---
type: Rust Method
title: property_tags
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1219-L1236
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_message_getprops_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/format_outlook_surface_folder_getprops_trace
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_view_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/unknown_property_wire_type_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/normalized_get_properties_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
---

# Signature

`pub(in crate::mapi) fn property_tags(&self) -> Vec<u32>`

# Called by

- [log_message_getprops_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_message_getprops_response_debug.md)
- [post_hierarchy_getprops_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_getprops_contract.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)
- [format_outlook_surface_folder_getprops_trace](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/format_outlook_surface_folder_getprops_trace.md)
- [log_get_properties_default_folder_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug.md)
- [log_get_properties_specific_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug.md)
- [log_get_properties_view_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_view_response_debug.md)
- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [unknown_property_wire_type_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/unknown_property_wire_type_response.md)
- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [append_delete_properties_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
- [normalized_get_properties_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/normalized_get_properties_request.md)
- [append_set_columns_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [set_columns_request_is_valid](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid.md)
- [set_columns_request_is_valid_for_rule_table](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [typed](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed.md)
- [simulate_table_access](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)