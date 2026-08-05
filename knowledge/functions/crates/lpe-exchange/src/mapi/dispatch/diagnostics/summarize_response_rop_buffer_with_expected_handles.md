---
type: Rust Function
title: summarize_response_rop_buffer_with_expected_handles
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L466-L476
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata
---

# Signature

`pub(in crate::mapi) fn summarize_response_rop_buffer_with_expected_handles( rop_buffer: &[u8], request_rop_ids: &[u8], expected_response_handle_indexes: &[Option<u8>], ) -> RopResponseDebugSummary`

# Calls

- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)

# Called by

- [log_execute_rop_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)
- [execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response.md)
- [execute_response_trace_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata.md)