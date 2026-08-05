---
type: Rust Function
title: read_response_error_code
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L927-L930
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_open_folder_probe_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_set_properties_probe_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
---

# Signature

`pub(super) fn read_response_error_code(responses: &[u8], offset: usize) -> Option<u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)
- [next_response_rop_start_validated](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated.md)
- [next_response_rop_start_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from.md)
- [next_response_rop_start](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start.md)
- [post_hierarchy_setprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract.md)
- [getprops_contract_response_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)
- [summarize_open_folder_probe_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_open_folder_probe_response.md)
- [summarize_get_properties_probe_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response.md)
- [summarize_set_properties_probe_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_set_properties_probe_response.md)
- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)