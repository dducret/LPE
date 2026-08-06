---
type: Rust Function
title: append_rop_save_changes_message_with_flags
resource: crates/lpe-exchange/src/tests/mod.rs#L15226-L15233
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_sync_import_save_reports_deleted_source_key
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_accepts_outlook_tombstone_maintenance_sequence
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_rejects_nonempty_tombstone_without_canonical_mapping
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_rejects_created_but_incomplete_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_direct_tombstone_set_is_staged_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_rejects_unmodeled_mixed_set_without_partial_stage
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_calendar_import_save_restores_containing_folder_response_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_failed_save_keeps_the_open_message_response_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message
---

# Signature

`fn append_rop_save_changes_message_with_flags( rops: &mut Vec<u8>, input: u8, response: u8, save_flags: u8, )`

# Called by

- [mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition.md)
- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread.md)
- [mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction.md)
- [mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq.md)
- [mapi_over_http_calendar_concurrent_rw_handles_require_force_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save.md)
- [mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid.md)
- [mapi_over_http_replays_outlook_contact_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save.md)
- [mapi_over_http_contact_sync_import_save_reports_deleted_source_key](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_sync_import_save_reports_deleted_source_key.md)
- [mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags.md)
- [mapi_over_http_local_freebusy_accepts_outlook_tombstone_maintenance_sequence](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_accepts_outlook_tombstone_maintenance_sequence.md)
- [mapi_over_http_local_freebusy_rejects_nonempty_tombstone_without_canonical_mapping](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_rejects_nonempty_tombstone_without_canonical_mapping.md)
- [mapi_over_http_local_freebusy_rejects_created_but_incomplete_tombstone](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_rejects_created_but_incomplete_tombstone.md)
- [mapi_over_http_local_freebusy_direct_tombstone_set_is_staged_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_direct_tombstone_set_is_staged_until_save.md)
- [mapi_over_http_local_freebusy_rejects_unmodeled_mixed_set_without_partial_stage](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/free_busy/mapi_over_http_local_freebusy_rejects_unmodeled_mixed_set_without_partial_stage.md)
- [mapi_over_http_online_associated_config_create_is_atomic_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql.md)
- [mapi_over_http_calendar_import_save_restores_containing_folder_response_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_calendar_import_save_restores_containing_folder_response_handle.md)
- [mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer.md)
- [mapi_over_http_failed_save_keeps_the_open_message_response_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_failed_save_keeps_the_open_message_response_handle.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)
- [mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email.md)
- [mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name.md)
- [mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql.md)
- [mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql.md)
- [mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists.md)
- [mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_replays_outlook_calendar_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save.md)
- [mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event.md)
- [execute_existing_calendar_sync_import](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import.md)
- [mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)
- [append_rop_save_changes_message](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message.md)