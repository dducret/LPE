---
type: Rust Function
title: post_hierarchy_action_summary
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L349-L428
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_phase
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_stall_code
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_progress
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_phase_name
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_stall_name
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_next_expected_phase
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/format_rop_ids_for_debug
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_close_kind
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/log_post_hierarchy_release_events
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_stays_empty_before_completed_hierarchy
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_create_save_object
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_submit_attempt_context
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_request_contracts
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_release
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_open
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_exports_hierarchy_query_position_context
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_release
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_findrow_release
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_exports_bootstrap_phase_scoreboard
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync
---

# Signature

`pub(in crate::mapi) fn post_hierarchy_action_summary( session: &MapiSession, disconnect_client_initiated: bool, ) -> PostHierarchyActionDebugSummary`

# Calls

- [outlook_bootstrap_phase](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_phase.md)
- [outlook_bootstrap_stall_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_stall_code.md)
- [record_mapi_outlook_view_bootstrap_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_progress.md)
- [outlook_bootstrap_phase_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_phase_name.md)
- [outlook_bootstrap_stall_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_stall_name.md)
- [outlook_bootstrap_next_expected_phase](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/outlook_bootstrap_next_expected_phase.md)
- [format_rop_ids_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/format_rop_ids_for_debug.md)
- [post_hierarchy_close_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_close_kind.md)

# Called by

- [log_execute_rop_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)
- [log_post_hierarchy_release_events](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/log_post_hierarchy_release_events.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)
- [post_hierarchy_action_summary_stays_empty_before_completed_hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_stays_empty_before_completed_hierarchy.md)
- [post_hierarchy_action_summary_records_execute_rops_and_client_actions](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions.md)
- [post_hierarchy_action_summary_records_last_create_save_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_create_save_object.md)
- [post_hierarchy_action_summary_records_submit_attempt_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_submit_attempt_context.md)
- [post_hierarchy_action_summary_records_last_request_contracts](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_request_contracts.md)
- [post_hierarchy_summary_tracks_create_save_after_visible_inbox_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_release.md)
- [post_hierarchy_summary_tracks_create_save_after_visible_inbox_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_open.md)
- [post_hierarchy_summary_exports_hierarchy_query_position_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_exports_hierarchy_query_position_context.md)
- [post_hierarchy_summary_counts_hierarchy_query_position_after_visible_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_release.md)
- [post_hierarchy_summary_counts_hierarchy_query_position_after_visible_findrow_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_findrow_release.md)
- [post_hierarchy_action_summary_exports_bootstrap_phase_scoreboard](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_exports_bootstrap_phase_scoreboard.md)
- [post_hierarchy_action_summary_classifies_release_logoff_without_content_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync.md)