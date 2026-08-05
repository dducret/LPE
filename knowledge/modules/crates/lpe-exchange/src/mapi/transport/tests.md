---
type: Rust Module
title: tests
resource: crates/lpe-exchange/src/mapi/transport/tests.rs#L1-L1749
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-notification-wait-acquire-notification-wait-active-session-request-notification-wait-empty-response-notification-wait-sleep-duration-notification-wait-streaming-response-mapi-notification-wait-maximum-wait-mapi-notification-wait-pending-period-millis
  - external/super
  - external/crate-mapi-transport-diagnostics-advertised-default-view-pending-open-is-primary-post-hierarchy-close-kind
  - external/crate-mapi-wire-ropid
  - external/tokio-stream-streamext
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [test_session](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/test_session.md)
- [test_session_for_outlook_startup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup.md)
- [test_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/test_principal.md)
- [execute_response_trace_metadata_summarizes_response_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_response_rops.md)
- [execute_response_trace_metadata_summarizes_mixed_multi_rop_execute](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_mixed_multi_rop_execute.md)
- [request_type_recognizes_get_hierarchy_info_as_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/request_type_recognizes_get_hierarchy_info_as_nspi_request.md)
- [connect_body_debug_summary_decodes_fields](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/connect_body_debug_summary_decodes_fields.md)
- [mapi_http_date_formats_imf_fixdate_in_gmt](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_http_date_formats_imf_fixdate_in_gmt.md)
- [mapi_response_debug_retains_logical_payload_for_outlook_trace](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_response_debug_retains_logical_payload_for_outlook_trace.md)
- [mapi_response_start_time_uses_current_http_date_not_sentinel](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_response_start_time_uses_current_http_date_not_sentinel.md)
- [execute_response_uses_one_exchange_chunked_processing_and_done_frame](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_uses_one_exchange_chunked_processing_and_done_frame.md)
- [notification_wait_empty_response_reports_success_with_empty_body](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_empty_response_reports_success_with_empty_body.md)
- [notification_wait_streaming_response_matches_exchange_completion_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_streaming_response_matches_exchange_completion_cookies.md)
- [regular_mapi_responses_include_exchange_routing_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/regular_mapi_responses_include_exchange_routing_cookies.md)
- [mapi_responses_advertise_the_default_pending_period](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_responses_advertise_the_default_pending_period.md)
- [accepted_response_rotates_the_mapi_sequence_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie.md)
- [ping_accepts_missing_or_prior_mapi_sequence_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/ping_accepts_missing_or_prior_mapi_sequence_cookie.md)
- [active_session_ping_failure_returns_current_session_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/active_session_ping_failure_returns_current_session_cookies.md)
- [notification_wait_uses_the_microsoft_five_minute_maximum](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_uses_the_microsoft_five_minute_maximum.md)
- [notification_wait_polls_before_the_pending_keepalive](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_polls_before_the_pending_keepalive.md)
- [notification_wait_active_session_acquire_waits_for_short_outlook_overlap](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_active_session_acquire_waits_for_short_outlook_overlap.md)
- [notification_wait_keeps_a_valid_session_during_execute_overlap](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_keeps_a_valid_session_during_execute_overlap.md)
- [session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection.md)
- [session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch.md)
- [post_hierarchy_action_summary_stays_empty_before_completed_hierarchy](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_stays_empty_before_completed_hierarchy.md)
- [post_hierarchy_action_summary_records_execute_rops_and_client_actions](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions.md)
- [post_hierarchy_action_summary_records_last_create_save_object](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_create_save_object.md)
- [post_hierarchy_action_summary_records_submit_attempt_context](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_submit_attempt_context.md)
- [post_hierarchy_action_summary_records_last_request_contracts](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_request_contracts.md)
- [partial_scope_checkpoint_not_stored_count_counts_expected_partial_scope_summaries](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/partial_scope_checkpoint_not_stored_count_counts_expected_partial_scope_summaries.md)
- [post_fai_inbox_probe_loop_terminal_summary_requires_no_normal_or_inbox_ics_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_fai_inbox_probe_loop_terminal_summary_requires_no_normal_or_inbox_ics_contents.md)
- [outlook_bootstrap_phase_classifies_current_wall_and_successful_progress](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/outlook_bootstrap_phase_classifies_current_wall_and_successful_progress.md)
- [outlook_bootstrap_stall_requires_inbox_content_sync](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/outlook_bootstrap_stall_requires_inbox_content_sync.md)
- [outlook_bootstrap_stall_classifies_post_common_views_notification_handoff](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/outlook_bootstrap_stall_classifies_post_common_views_notification_handoff.md)
- [outlook_bootstrap_stall_classifies_exact_fai_findrow_without_open](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/outlook_bootstrap_stall_classifies_exact_fai_findrow_without_open.md)
- [post_hierarchy_close_kind_classifies_visible_inbox_query_position_without_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_classifies_visible_inbox_query_position_without_query_rows.md)
- [post_hierarchy_close_kind_prioritizes_visible_inbox_release_without_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_prioritizes_visible_inbox_release_without_query_rows.md)
- [visible_inbox_findrow_suppresses_release_without_query_rows_diagnostic](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/visible_inbox_findrow_suppresses_release_without_query_rows_diagnostic.md)
- [post_hierarchy_summary_tracks_create_save_after_visible_inbox_release](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_release.md)
- [post_hierarchy_summary_tracks_create_save_after_visible_inbox_open](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_open.md)
- [post_hierarchy_summary_exports_hierarchy_query_position_context](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_exports_hierarchy_query_position_context.md)
- [post_hierarchy_summary_counts_hierarchy_query_position_after_visible_release](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_release.md)
- [post_hierarchy_summary_counts_hierarchy_query_position_after_visible_findrow_release](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_findrow_release.md)
- [default_view_query_rows_does_not_clear_visible_inbox_release_diagnostic](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/default_view_query_rows_does_not_clear_visible_inbox_release_diagnostic.md)
- [post_hierarchy_close_kind_classifies_calendar_query_position_without_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_classifies_calendar_query_position_without_query_rows.md)
- [post_hierarchy_close_kind_classifies_calendar_named_property_burst_without_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_classifies_calendar_named_property_burst_without_query_rows.md)
- [post_hierarchy_close_kind_classifies_umolk_named_property_burst](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_classifies_umolk_named_property_burst.md)
- [post_hierarchy_close_kind_prioritizes_umolk_over_visible_inbox_release](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_prioritizes_umolk_over_visible_inbox_release.md)
- [post_hierarchy_close_kind_classifies_default_view_hierarchy_query_position](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_classifies_default_view_hierarchy_query_position.md)
- [post_hierarchy_close_kind_classifies_visible_inbox_message_faults](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_classifies_visible_inbox_message_faults.md)
- [post_hierarchy_close_kind_classifies_default_view_sweep_before_inbox_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_classifies_default_view_sweep_before_inbox_query_rows.md)
- [post_hierarchy_close_kind_classifies_default_view_followup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_close_kind_classifies_default_view_followup.md)
- [advertised_default_view_pending_open_is_primary_without_visible_inbox_release](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/advertised_default_view_pending_open_is_primary_without_visible_inbox_release.md)
- [advertised_default_view_pending_open_is_not_primary_after_visible_inbox_release](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/advertised_default_view_pending_open_is_not_primary_after_visible_inbox_release.md)
- [records_default_view_normal_query_rows_without_marking_inbox_complete](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/records_default_view_normal_query_rows_without_marking_inbox_complete.md)
- [post_hierarchy_action_summary_exports_bootstrap_phase_scoreboard](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_exports_bootstrap_phase_scoreboard.md)
- [special_folder_contract_summary_reports_conversation_history](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/special_folder_contract_summary_reports_conversation_history.md)
- [required_default_folder_disconnect_coverage_reports_calendar_contacts_gap](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/required_default_folder_disconnect_coverage_reports_calendar_contacts_gap.md)
- [post_hierarchy_action_summary_classifies_release_logoff_without_content_sync](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync.md)
- [post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe.md)

# Imports

- `super::notification_wait::{
    acquire_notification_wait_active_session_request, notification_wait_empty_response,
    notification_wait_sleep_duration, notification_wait_streaming_response,
    MAPI_NOTIFICATION_WAIT_MAXIMUM_WAIT, MAPI_NOTIFICATION_WAIT_PENDING_PERIOD_MILLIS,
}`
- `super::*`
- `crate::mapi::transport::diagnostics::{
    advertised_default_view_pending_open_is_primary, post_hierarchy_close_kind,
}`
- `crate::mapi::wire::RopId`
- `tokio_stream::StreamExt`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)