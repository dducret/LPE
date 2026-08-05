---
type: Rust Module
title: execute
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L1-L514
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [ExecuteRequest](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/execute/ExecuteRequest.md)
- [parse_execute_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request.md)
- [acquire_execute_active_session_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/acquire_execute_active_session_request.md)
- [rop_buffer_is_store_independent_release_only](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_release_only.md)
- [execute_can_skip_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope.md)
- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [is_store_independent_folder_getprops_probe](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_folder_getprops_probe.md)
- [is_store_independent_special_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_special_folder.md)
- [rop_buffer_has_no_requests](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_has_no_requests.md)
- [execute_success_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_success_rop_buffer.md)
- [apply_execute_max_rop_out](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out.md)
- [execute_response_exceeds_max_rop_out](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_exceeds_max_rop_out.md)
- [restore_pending_notifications_after_execute_overflow](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/restore_pending_notifications_after_execute_overflow.md)
- [available_execute_rop_response_size](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/available_execute_rop_response_size.md)
- [execute_response_handle_table](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_handle_table.md)
- [parse_execute_rop_dispatch_input](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input.md)
- [record_execute_stream_batch_observation](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_stream_batch_observation.md)
- [read_next_execute_rop_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/read_next_execute_rop_request.md)
- [finalize_execute_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/finalize_execute_rop_buffer.md)
- [record_execute_sync_observations](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_sync_observations.md)
- [abort_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/abort_response.md)
- [append_abort_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_abort_response.md)
- [progress_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/progress_response.md)
- [append_progress_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_progress_response.md)
- [reset_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/reset_table_response.md)
- [append_reset_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_reset_table_response.md)
- [append_execute_status_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)
- [unknown_property_wire_type_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/unknown_property_wire_type_response.md)

# Imports

- `super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)