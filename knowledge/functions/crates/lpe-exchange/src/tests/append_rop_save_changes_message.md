---
type: Rust Function
title: append_rop_save_changes_message
resource: crates/lpe-exchange/src/tests/mod.rs#L15353-L15355
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/save_staged_calendar_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_advertised_calendar_create_uses_default_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_task_crud_uses_canonical_tasks
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_set_properties_updates_canonical_mail_reminder_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_conversation_action_applies_to_future_matching_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_conversation_action_cross_store_keeps_local_message_in_place
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_delete_attachment_is_committed_by_save_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_stale_message_handle_requires_force_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_notification_wait_reports_content_event_after_registered_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_link_copy_to_uses_message_content_root
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_create_resolves_named_email_addresses
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_crud_uses_canonical_contacts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_created_contact_link_config_accepts_outlook_marker_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_properties_round_trip_on_canonical_item_kinds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_property_set_before_save_persists_on_created_item
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxcmsg_setting_message_properties_preserves_html_cid_body
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_body_stream_writes_canonical_message_body
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_message_properties_commit_on_save_changes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_set_properties_validates_swapped_todo_data
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_common_view_named_view_accepts_microsoft_descriptor_write_stream_batch
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_fast_transfer_destination_rejects_partial_property_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxcprpt_stream_release_then_message_save_persists_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_mutations_are_cumulative_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_to_copies_custom_values_excluding_tags
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_properties_copies_custom_values_and_reports_missing_tags
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_create_message_rejects_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_set_properties_updates_canonical_post_item
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_body_stream_updates_canonical_post_item
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_sync_import_creates_canonical_post_item
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_example_saves_canonically
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_partial_item_upload_updates_existing_message_without_import
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_message_uploads_do_not_create_visible_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_persisted_associated_config_write_preserves_class_on_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_trash_collector_import_then_save
---

# Signature

`fn append_rop_save_changes_message(rops: &mut Vec<u8>, input: u8, response: u8)`

# Calls

- [append_rop_save_changes_message_with_flags](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags.md)

# Called by

- [mapi_over_http_set_properties_updates_canonical_event_and_task_reminders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders.md)
- [save_staged_calendar_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/save_staged_calendar_event.md)
- [mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle.md)
- [mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles.md)
- [mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids.md)
- [mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids.md)
- [mapi_over_http_empty_advertised_calendar_create_uses_default_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_advertised_calendar_create_uses_default_collection.md)
- [mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event.md)
- [mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties.md)
- [mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request.md)
- [mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local.md)
- [mapi_over_http_calendar_create_commits_event_and_attachment_together](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together.md)
- [mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons.md)
- [mapi_over_http_task_crud_uses_canonical_tasks](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_task_crud_uses_canonical_tasks.md)
- [mapi_over_http_set_properties_updates_canonical_mail_reminder_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_set_properties_updates_canonical_mail_reminder_state.md)
- [mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_conversation_action_applies_to_future_matching_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_conversation_action_applies_to_future_matching_message.md)
- [mapi_over_http_conversation_action_cross_store_keeps_local_message_in_place](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_conversation_action_cross_store_keeps_local_message_in_place.md)
- [mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end.md)
- [mapi_over_http_microsoft_delete_attachment_is_committed_by_save_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_delete_attachment_is_committed_by_save_message.md)
- [mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode.md)
- [mapi_over_http_microsoft_stale_message_handle_requires_force_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_stale_message_handle_requires_force_save.md)
- [mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds.md)
- [mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly.md)
- [mapi_over_http_notification_wait_reports_content_event_after_registered_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_notification_wait_reports_content_event_after_registered_save.md)
- [mapi_over_http_contact_link_copy_to_uses_message_content_root](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_link_copy_to_uses_message_content_root.md)
- [mapi_over_http_outlook_contact_create_resolves_named_email_addresses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_create_resolves_named_email_addresses.md)
- [mapi_over_http_contact_crud_uses_canonical_contacts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_crud_uses_canonical_contacts.md)
- [mapi_over_http_created_contact_link_config_accepts_outlook_marker_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_created_contact_link_config_accepts_outlook_marker_property.md)
- [mapi_over_http_custom_named_properties_round_trip_on_canonical_item_kinds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_properties_round_trip_on_canonical_item_kinds.md)
- [mapi_over_http_custom_named_property_set_before_save_persists_on_created_item](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_property_set_before_save_persists_on_created_item.md)
- [mapi_over_http_microsoft_oxcmsg_setting_message_properties_preserves_html_cid_body](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxcmsg_setting_message_properties_preserves_html_cid_body.md)
- [mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property.md)
- [mapi_over_http_string8_body_stream_writes_canonical_message_body](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_body_stream_writes_canonical_message_body.md)
- [mapi_over_http_microsoft_message_properties_commit_on_save_changes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_message_properties_commit_on_save_changes.md)
- [mapi_over_http_set_properties_validates_swapped_todo_data](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_set_properties_validates_swapped_todo_data.md)
- [mapi_over_http_common_view_named_view_accepts_microsoft_descriptor_write_stream_batch](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_common_view_named_view_accepts_microsoft_descriptor_write_stream_batch.md)
- [mapi_over_http_fast_transfer_destination_rejects_partial_property_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_fast_transfer_destination_rejects_partial_property_buffer.md)
- [mapi_over_http_microsoft_oxcprpt_stream_release_then_message_save_persists_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxcprpt_stream_release_then_message_save_persists_property.md)
- [mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql.md)
- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_over_http_associated_config_mutations_are_cumulative_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_mutations_are_cumulative_until_save.md)
- [mapi_over_http_microsoft_copy_to_copies_custom_values_excluding_tags](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_to_copies_custom_values_excluding_tags.md)
- [mapi_over_http_microsoft_copy_properties_copies_custom_values_and_reports_missing_tags](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_properties_copies_custom_values_and_reports_missing_tags.md)
- [mapi_over_http_public_folder_create_message_rejects_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_create_message_rejects_recipients.md)
- [mapi_over_http_public_folder_set_properties_updates_canonical_post_item](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_set_properties_updates_canonical_post_item.md)
- [mapi_over_http_public_folder_body_stream_updates_canonical_post_item](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_body_stream_updates_canonical_post_item.md)
- [mapi_over_http_public_folder_sync_import_creates_canonical_post_item](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_sync_import_creates_canonical_post_item.md)
- [mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type.md)
- [mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically.md)
- [mapi_over_http_modify_recipients_x500_rows_save_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically.md)
- [mapi_over_http_microsoft_modify_recipients_example_saves_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_example_saves_canonically.md)
- [mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation.md)
- [mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save.md)
- [mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct.md)
- [mapi_over_http_microsoft_partial_item_upload_updates_existing_message_without_import](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_partial_item_upload_updates_existing_message_without_import.md)
- [mapi_over_http_associated_message_uploads_do_not_create_visible_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_message_uploads_do_not_create_visible_items.md)
- [mapi_over_http_sync_import_associated_message_persists_and_replays_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai.md)
- [mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai.md)
- [mapi_over_http_persisted_associated_config_write_preserves_class_on_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_persisted_associated_config_write_preserves_class_on_save.md)
- [mapi_over_http_replays_outlook_trash_collector_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_trash_collector_import_then_save.md)