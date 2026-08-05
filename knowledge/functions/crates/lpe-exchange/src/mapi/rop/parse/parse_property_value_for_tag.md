---
type: Rust Function
title: parse_property_value_for_tag
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1486-L1491
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_response_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/summarize_message_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/extract_getprops_binary_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_flagged_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_response_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_view_response_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
---

# Signature

`pub(in crate::mapi) fn parse_property_value_for_tag( cursor: &mut Cursor<'_>, property_tag: u32, ) -> Result<MapiValue>`

# Calls

- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)

# Called by

- [default_folder_getprops_response_values_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_response_values_for_debug.md)
- [summarize_message_getprops_materialization](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/summarize_message_getprops_materialization.md)
- [getprops_contract_response_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary.md)
- [summarize_get_properties_probe_response_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response_values.md)
- [extract_getprops_binary_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/extract_getprops_binary_value.md)
- [summarize_flagged_getprops_materialization](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_flagged_getprops_materialization.md)
- [get_properties_specific_response_values_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_response_values_for_debug.md)
- [get_properties_view_response_values_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_view_response_values_for_debug.md)
- [parse_tagged_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)
- [parse_simple_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row.md)
- [parse_wrapped_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)