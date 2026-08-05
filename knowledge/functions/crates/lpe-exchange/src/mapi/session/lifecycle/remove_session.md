---
type: Rust Function
title: remove_session
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L289-L296
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/folder_profile_tombstones_are_handle_local_and_clear_on_set
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_rejects_active_context
  - functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_a_session_context_for_another_endpoint_or_principal
  - functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_replaces_the_prior_emsmdb_context
  - functions/crates/lpe-exchange/src/mapi/session/tests/execute_replay_cache_evicts_oldest_inserted_request_id
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_records_transport_request_lifetime
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_collaboration_content_change_for_active_root_depth_hierarchy_table_without_counts
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions
  - functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables
  - functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_detects_abandon_after_inbox_fai_query_rows_release
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_deleted_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_saved_search_folder_definition
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_deleted_saved_search_folder_definition
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_records_completed_sync_checkpoint_once
  - functions/crates/lpe-exchange/src/mapi/session/tests/release_handle_slot_forgets_folder_profile_property_tombstones
  - functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_prefers_free_low_output_slot_handle
  - functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_skips_reserved_same_execute_handle
  - functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_does_not_reuse_old_low_slot_handle
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_named_property_updates_bidirectional_registry
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_calendar_named_property_keeps_registered_mailbox_id
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_named_property_rejects_property_id_reuse
  - functions/crates/lpe-exchange/src/mapi/session/tests/dynamic_named_property_allocation_starts_at_project_dynamic_range
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_well_known_named_property_keeps_registered_dynamic_id
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_well_known_named_property_keeps_registered_reserved_range_id
  - functions/crates/lpe-exchange/src/mapi/session/tests/cached_named_property_can_use_a_well_known_fallback_id
  - functions/crates/lpe-exchange/src/mapi/session/tests/ps_mapi_lid_maps_directly_even_in_named_property_range
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending
  - functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/tests/ping_accepts_missing_or_prior_mapi_sequence_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/tests/active_session_ping_failure_returns_current_session_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_keeps_a_valid_session_during_execute_overlap
  - functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection
  - functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch
---

# Signature

`pub(in crate::mapi) fn remove_session(session_id: &str) -> Option<MapiSession>`

# Calls

- [sessions](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions.md)
- [prune_expired_sessions_locked](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [folder_profile_tombstones_are_handle_local_and_clear_on_set](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/folder_profile_tombstones_are_handle_local_and_clear_on_set.md)
- [reconnect_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [reconnect_session_rejects_active_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_rejects_active_context.md)
- [connect_rejects_a_session_context_for_another_endpoint_or_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_a_session_context_for_another_endpoint_or_principal.md)
- [reconnect_session_replaces_the_prior_emsmdb_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_replaces_the_prior_emsmdb_context.md)
- [execute_replay_cache_evicts_oldest_inserted_request_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/execute_replay_cache_evicts_oldest_inserted_request_id.md)
- [session_records_transport_request_lifetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_records_transport_request_lifetime.md)
- [session_retains_folder_count_change_for_active_parent_hierarchy_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table.md)
- [session_retains_collaboration_content_change_for_active_root_depth_hierarchy_table_without_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_collaboration_content_change_for_active_root_depth_hierarchy_table_without_counts.md)
- [session_delivers_only_complete_message_moves_and_copies_to_subscriptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions.md)
- [hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables.md)
- [notification_subscription_preserves_rop_logon_id_through_rop_notify](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify.md)
- [session_detects_abandon_after_inbox_fai_query_rows_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_detects_abandon_after_inbox_fai_query_rows_release.md)
- [session_does_not_treat_findrow_delivered_fai_as_abandoned](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned.md)
- [session_remembers_deleted_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_deleted_advertised_special_folder.md)
- [session_remembers_saved_search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_saved_search_folder_definition.md)
- [session_remembers_deleted_saved_search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_deleted_saved_search_folder_definition.md)
- [session_records_completed_sync_checkpoint_once](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_records_completed_sync_checkpoint_once.md)
- [release_handle_slot_forgets_folder_profile_property_tombstones](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/release_handle_slot_forgets_folder_profile_property_tombstones.md)
- [allocate_output_handle_prefers_free_low_output_slot_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_prefers_free_low_output_slot_handle.md)
- [allocate_output_handle_skips_reserved_same_execute_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_skips_reserved_same_execute_handle.md)
- [allocate_output_handle_does_not_reuse_old_low_slot_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_does_not_reuse_old_low_slot_handle.md)
- [cached_named_property_updates_bidirectional_registry](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_named_property_updates_bidirectional_registry.md)
- [cached_calendar_named_property_keeps_registered_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_calendar_named_property_keeps_registered_mailbox_id.md)
- [cached_named_property_rejects_property_id_reuse](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_named_property_rejects_property_id_reuse.md)
- [dynamic_named_property_allocation_starts_at_project_dynamic_range](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/dynamic_named_property_allocation_starts_at_project_dynamic_range.md)
- [cached_well_known_named_property_keeps_registered_dynamic_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_well_known_named_property_keeps_registered_dynamic_id.md)
- [cached_well_known_named_property_keeps_registered_reserved_range_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_well_known_named_property_keeps_registered_reserved_range_id.md)
- [cached_named_property_can_use_a_well_known_fallback_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/cached_named_property_can_use_a_well_known_fallback_id.md)
- [ps_mapi_lid_maps_directly_even_in_named_property_range](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/ps_mapi_lid_maps_directly_even_in_named_property_range.md)
- [disconnect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [notification_wait_event_pending](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending.md)
- [accepted_response_rotates_the_mapi_sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie.md)
- [ping_accepts_missing_or_prior_mapi_sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/ping_accepts_missing_or_prior_mapi_sequence_cookie.md)
- [active_session_ping_failure_returns_current_session_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/active_session_ping_failure_returns_current_session_cookies.md)
- [notification_wait_keeps_a_valid_session_during_execute_overlap](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_keeps_a_valid_session_during_execute_overlap.md)
- [session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection.md)
- [session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch.md)