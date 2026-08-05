---
type: Rust Function
title: next_response_rop_start_from
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L683-L726
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_handle_index_matches
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_fixed_frame_end
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
---

# Signature

`fn next_response_rop_start_from( responses: &[u8], search_start: usize, expected_rop_id: u8, expected_response_handle_index: Option<u8>, following_expected_rop_id: Option<u8>, ) -> Option<usize>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [read_response_error_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)
- [response_handle_index_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_handle_index_matches.md)
- [response_rop_fixed_frame_end](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_fixed_frame_end.md)

# Called by

- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)