---
type: Rust Function
title: append_mapi_i32_property
resource: crates/lpe-exchange/src/tests/mod.rs#L14933-L14936
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_reject_unsupported_bits_without_side_effect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_task_crud_uses_canonical_tasks
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_create_inline_attachment_preserves_content_id_metadata
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_stale_message_handle_requires_force_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_created_contact_link_config_accepts_outlook_marker_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_attach_text_file_stream_saves_canonical_attachment
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_message_properties_commit_on_save_changes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_subrestriction_matches_message_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_create_associated_navigation_shortcut_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_message_change_updates_canonical_flags
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_message_uploads_do_not_create_visible_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
---

# Signature

`fn append_mapi_i32_property(values: &mut Vec<u8>, property_tag: u32, value: i32)`

# Called by

- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request.md)
- [mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status.md)
- [mapi_over_http_calendar_state_flags_reject_unsupported_bits_without_side_effect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_reject_unsupported_bits_without_side_effect.md)
- [mapi_over_http_task_crud_uses_canonical_tasks](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_task_crud_uses_canonical_tasks.md)
- [mapi_over_http_create_inline_attachment_preserves_content_id_metadata](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_create_inline_attachment_preserves_content_id_metadata.md)
- [mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode.md)
- [mapi_over_http_microsoft_stale_message_handle_requires_force_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_stale_message_handle_requires_force_save.md)
- [mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds.md)
- [mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads.md)
- [mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly.md)
- [mapi_over_http_created_contact_link_config_accepts_outlook_marker_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_created_contact_link_config_accepts_outlook_marker_property.md)
- [mapi_over_http_microsoft_attach_text_file_stream_saves_canonical_attachment](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_attach_text_file_stream_saves_canonical_attachment.md)
- [mapi_over_http_microsoft_message_properties_commit_on_save_changes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_message_properties_commit_on_save_changes.md)
- [mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer.md)
- [mapi_over_http_microsoft_subrestriction_matches_message_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_subrestriction_matches_message_recipients.md)
- [mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation.md)
- [mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save.md)
- [mapi_over_http_common_views_create_associated_navigation_shortcut_persists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_create_associated_navigation_shortcut_persists.md)
- [mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name.md)
- [mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save.md)
- [mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists.md)
- [mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save.md)
- [mapi_over_http_sync_import_message_change_updates_canonical_flags](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_message_change_updates_canonical_flags.md)
- [mapi_over_http_associated_message_uploads_do_not_create_visible_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_message_uploads_do_not_create_visible_items.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai.md)
- [mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)