---
type: Rust Module
title: execute
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L1-L1266
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-super
  - external/super
  - external/crate-mapi-wire-mapinotificationeventmask
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [execute_max_rop_out_returns_buffer_too_small_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_returns_buffer_too_small_response.md)
- [execute_overflow_restores_deliverable_notification_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch.md)
- [execute_overflow_does_not_restore_unmatched_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification.md)
- [execute_max_rop_out_preserves_extended_buffer_for_generic_overflow](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_preserves_extended_buffer_for_generic_overflow.md)
- [parse_execute_request_keeps_max_rop_out](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/parse_execute_request_keeps_max_rop_out.md)
- [parse_execute_request_preserves_chain_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/parse_execute_request_preserves_chain_flag.md)
- [execute_response_budget_reserves_extended_framing_and_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_response_budget_reserves_extended_framing_and_handle_table.md)
- [automatic_fast_transfer_buffer_uses_execute_residual_output_budget](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget.md)
- [chained_fast_transfer_get_buffer_repeats_handles_until_done](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done.md)
- [execute_stall_warning_requires_specific_post_hierarchy_pre_sync_stop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_stall_warning_requires_specific_post_hierarchy_pre_sync_stop.md)
- [execute_active_session_acquire_waits_for_short_outlook_overlap](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_active_session_acquire_waits_for_short_outlook_overlap.md)
- [release_only_execute_batch_is_store_independent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_batch_is_store_independent.md)
- [release_only_execute_with_notification_target_requires_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_with_notification_target_requires_identity_scope.md)
- [release_only_execute_response_uses_exchange_released_handle_sentinel](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_response_uses_exchange_released_handle_sentinel.md)
- [release_with_appended_notification_uses_exchange_released_handle_sentinel](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_with_appended_notification_uses_exchange_released_handle_sentinel.md)
- [mixed_release_execute_response_preserves_sparse_output_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_release_execute_response_preserves_sparse_output_handle_index.md)
- [mixed_create_save_batch_preserves_save_response_folder_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_create_save_batch_preserves_save_response_folder_handle_slot.md)
- [mixed_setcolumns_release_response_omits_release_only_handle_slots](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_release_response_omits_release_only_handle_slots.md)
- [mixed_setcolumns_release_response_trims_snapshot_to_response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_release_response_trims_snapshot_to_response_handle_index.md)
- [mixed_setcolumns_trailing_release_returns_invalid_released_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_trailing_release_returns_invalid_released_handle.md)
- [outlook_setcolumns_then_release_same_slot_returns_post_release_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/outlook_setcolumns_then_release_same_slot_returns_post_release_handle_table.md)
- [non_release_echo_response_keeps_output_placeholders](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/non_release_echo_response_keeps_output_placeholders.md)
- [mixed_release_response_keeps_unreleased_sparse_output_holes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_release_response_keeps_unreleased_sparse_output_holes.md)
- [execute_rop_debug_summary_decodes_ids_and_return_codes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_decodes_ids_and_return_codes.md)
- [execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response.md)
- [execute_rop_debug_summary_distinguishes_truncated_release_prefix](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_distinguishes_truncated_release_prefix.md)
- [execute_rop_response_summary_uses_full_truncated_request_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids.md)
- [get_buffer_response_debug_exposes_wire_framing](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/get_buffer_response_debug_exposes_wire_framing.md)
- [execute_rop_debug_summary_skips_release_rops_without_responses](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_release_rops_without_responses.md)
- [execute_rop_response_summary_keeps_get_address_types_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_address_types_frame_boundary.md)
- [execute_rop_response_summary_keeps_get_property_ids_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_property_ids_frame_boundary.md)
- [execute_rop_response_summary_keeps_contents_table_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_contents_table_frame_boundary.md)
- [execute_rop_response_summary_skips_implausible_query_rows_payload_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_query_rows_payload_marker.md)
- [execute_rop_response_summary_keeps_create_setprops_save_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_create_setprops_save_frame_boundary.md)
- [execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop.md)
- [execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker.md)
- [execute_rop_response_summary_skips_bare_warning_getprops_payload_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_skips_bare_warning_getprops_payload_marker.md)
- [execute_rop_response_framing_summary_marks_multi_rop_boundaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_framing_summary_marks_multi_rop_boundaries.md)
- [execute_response_framing_context_includes_bootstrap_getprops_batches](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_response_framing_context_includes_bootstrap_getprops_batches.md)

# Imports

- `super::super::*`
- `super::*`
- `crate::mapi::wire::MapiNotificationEventMask`

# Member of

- [lpe-exchange](../../../../../../../packages/crates/lpe-exchange.md)