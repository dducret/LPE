---
type: Rust Function
title: log_execute_rop_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute.rs#L9-L601
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_missing_reason
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_release_request_shape
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/normalized_rop_sequence_signature
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/should_log_execute_stalled_before_content_sync
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute_response_framing_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute_batch_has_same_save_getprops_not_found
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_execute_rop_debug( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, _session_id: &str, request_id: &str, request: &RopRequestDebugSummary, request_rop_buffer: &[u8], response_rop_buffer: &[u8], session: &MapiSession, post_hierarchy_observation: PostHierarchyExecuteObservation, )`

# Calls

- [summarize_response_rop_buffer_with_expected_handles](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles.md)
- [summarize_logon_response_rop](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)
- [post_hierarchy_action_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary.md)
- [outlook_startup_gate_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)
- [normal_inbox_visible_row_missing_reason](../../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_missing_reason.md)
- [normal_inbox_visible_row_release_request_shape](../../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_release_request_shape.md)
- [normalized_rop_sequence_signature](../../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normalized_rop_sequence_signature.md)
- [should_log_execute_stalled_before_content_sync](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/should_log_execute_stalled_before_content_sync.md)
- [current_store_replica_guid](../../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [execute_response_framing_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute_response_framing_context.md)
- [execute_batch_has_same_save_getprops_not_found](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute_batch_has_same_save_getprops_not_found.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)

# Called by

- [execute_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)