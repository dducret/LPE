---
type: Rust Function
title: rop_buffer_with_response_spec
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L103-L115
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/finalize_execute_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/packed_fast_transfer_source_get_buffer_response_payloads
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_returns_buffer_too_small_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_preserves_extended_buffer_for_generic_overflow
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_address_types_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_property_ids_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_create_setprops_save_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_bare_warning_getprops_payload_marker
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_framing_summary_marks_multi_rop_boundaries
---

# Signature

`pub(in crate::mapi) fn rop_buffer_with_response_spec( response: Vec<u8>, output_handles: &[u32], ) -> Vec<u8>`

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [parse_execute_rop_dispatch_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input.md)
- [finalize_execute_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/finalize_execute_rop_buffer.md)
- [packed_fast_transfer_source_get_buffer_response_payloads](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/packed_fast_transfer_source_get_buffer_response_payloads.md)
- [execute_max_rop_out_returns_buffer_too_small_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_returns_buffer_too_small_response.md)
- [execute_max_rop_out_preserves_extended_buffer_for_generic_overflow](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_preserves_extended_buffer_for_generic_overflow.md)
- [automatic_fast_transfer_buffer_uses_execute_residual_output_budget](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget.md)
- [chained_fast_transfer_get_buffer_repeats_handles_until_done](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done.md)
- [execute_rop_response_summary_uses_full_truncated_request_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids.md)
- [execute_rop_response_summary_keeps_get_address_types_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_address_types_frame_boundary.md)
- [execute_rop_response_summary_keeps_get_property_ids_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_property_ids_frame_boundary.md)
- [execute_rop_response_summary_keeps_contents_table_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary.md)
- [execute_rop_response_summary_skips_implausible_query_rows_payload_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker.md)
- [execute_rop_response_summary_keeps_create_setprops_save_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_create_setprops_save_frame_boundary.md)
- [execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop.md)
- [execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker.md)
- [execute_rop_response_summary_skips_bare_warning_getprops_payload_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_bare_warning_getprops_payload_marker.md)
- [execute_rop_response_framing_summary_marks_multi_rop_boundaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_framing_summary_marks_multi_rop_boundaries.md)