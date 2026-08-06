---
type: Rust Function
title: response_rops_and_handles_from_execute_body
resource: crates/lpe-exchange/src/tests/mod.rs#L15593-L15611
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_same_folder_move_is_idempotent
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_properties_survive_restart_style_session
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attendee_named_properties_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_display_cc_updates_optional_attendees
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_description_updates_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_common_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_stale_message_handle_requires_force_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_sync_import_save_reports_deleted_source_key
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_outlook_set_message_read_flag_accepts_default_flag
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_outlook_set_message_read_flag_accepts_clear_read_flag
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/rule_organizer/mapi_over_http_exchange_rule_organizer_query_rows_opens_returned_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_calendar_import_save_restores_containing_folder_response_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_failed_save_keeps_the_open_message_response_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_import_move_to_deleted_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_fai_by_outlook_source_key
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_failed_set_columns_invalidates_table_until_success
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_failed_sort_and_restrict_invalidate_table_until_success
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
---

# Signature

`fn response_rops_and_handles_from_execute_body(body: &[u8]) -> (Vec<u8>, Vec<u32>)`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_over_http_calendar_same_folder_move_is_idempotent](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_same_folder_move_is_idempotent.md)
- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_calendar_custom_properties_survive_restart_style_session](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_properties_survive_restart_style_session.md)
- [mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields.md)
- [mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder.md)
- [mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition.md)
- [mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle.md)
- [mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay.md)
- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards.md)
- [mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction.md)
- [mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream.md)
- [mapi_over_http_calendar_concurrent_rw_handles_require_force_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save.md)
- [mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid.md)
- [mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete.md)
- [mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status.md)
- [mapi_over_http_calendar_attendee_named_properties_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attendee_named_properties_update_canonical_event.md)
- [mapi_over_http_calendar_display_cc_updates_optional_attendees](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_display_cc_updates_optional_attendees.md)
- [mapi_over_http_calendar_time_zone_description_updates_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_description_updates_canonical_event.md)
- [mapi_over_http_calendar_whole_start_end_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_common_start_end_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_common_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status.md)
- [mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local.md)
- [mapi_over_http_calendar_create_commits_event_and_attachment_together](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together.md)
- [mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons.md)
- [mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode.md)
- [mapi_over_http_microsoft_stale_message_handle_requires_force_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_stale_message_handle_requires_force_save.md)
- [mapi_over_http_replays_outlook_contact_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save.md)
- [mapi_over_http_contact_sync_import_save_reports_deleted_source_key](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_sync_import_save_reports_deleted_source_key.md)
- [mapi_over_http_outlook_set_message_read_flag_accepts_default_flag](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_outlook_set_message_read_flag_accepts_default_flag.md)
- [mapi_over_http_outlook_set_message_read_flag_accepts_clear_read_flag](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_outlook_set_message_read_flag_accepts_clear_read_flag.md)
- [mapi_over_http_exchange_rule_organizer_query_rows_opens_returned_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/rule_organizer/mapi_over_http_exchange_rule_organizer_query_rows_opens_returned_message.md)
- [mapi_over_http_calendar_import_save_restores_containing_folder_response_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_calendar_import_save_restores_containing_folder_response_handle.md)
- [mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer.md)
- [mapi_over_http_failed_save_keeps_the_open_message_response_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_failed_save_keeps_the_open_message_response_handle.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql.md)
- [mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql.md)
- [mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists.md)
- [mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_replays_outlook_calendar_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save.md)
- [mapi_over_http_replays_outlook_calendar_import_move_to_deleted_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_import_move_to_deleted_items.md)
- [mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event.md)
- [execute_existing_calendar_sync_import](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import.md)
- [mapi_over_http_sync_import_deletes_removes_fai_by_outlook_source_key](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_fai_by_outlook_source_key.md)
- [mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads.md)
- [mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink.md)
- [mapi_over_http_microsoft_failed_set_columns_invalidates_table_until_success](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_failed_set_columns_invalidates_table_until_success.md)
- [mapi_over_http_microsoft_failed_sort_and_restrict_invalidate_table_until_success](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_failed_sort_and_restrict_invalidate_table_until_success.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)
- [mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence.md)
- [response_rops_from_execute_response](../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)