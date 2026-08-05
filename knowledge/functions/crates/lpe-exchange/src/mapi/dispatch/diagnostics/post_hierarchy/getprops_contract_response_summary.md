---
type: Rust Function
title: getprops_contract_response_summary
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L209-L291
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_response_values_for_debug
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/mapi_value_is_zero_or_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/format_outlook_surface_folder_getprops_trace
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/getprops_contract_response_summary_includes_access_value
---

# Signature

`pub(in crate::mapi::dispatch) fn getprops_contract_response_summary( property_tags: &[u32], response: &[u8], ) -> GetPropsContractResponseSummary`

# Calls

- [read_response_error_code](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)
- [get_properties_specific_response_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_response_values_for_debug.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [read_u8](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_property_value_for_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)
- [mapi_value_is_zero_or_default](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/mapi_value_is_zero_or_default.md)

# Called by

- [post_hierarchy_getprops_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_getprops_contract.md)
- [format_outlook_surface_folder_getprops_trace](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/format_outlook_surface_folder_getprops_trace.md)
- [getprops_contract_response_summary_includes_access_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/getprops_contract_response_summary_includes_access_value.md)