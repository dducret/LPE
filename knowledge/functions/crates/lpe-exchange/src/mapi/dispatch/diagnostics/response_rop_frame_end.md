---
type: Rust Function
title: response_rop_frame_end
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L558-L588
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_fixed_frame_end
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
---

# Signature

`pub(in crate::mapi::dispatch) fn response_rop_frame_end( responses: &[u8], start: usize, error_code: Option<u32>, next_expected_rop_id: Option<u8>, next_expected_response_handle_index: Option<u8>, following_expected_rop_id: Option<u8>, ) -> usize`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [response_rop_fixed_frame_end](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_fixed_frame_end.md)
- [next_response_rop_start_validated](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated.md)
- [next_response_rop_start](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start.md)

# Called by

- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)