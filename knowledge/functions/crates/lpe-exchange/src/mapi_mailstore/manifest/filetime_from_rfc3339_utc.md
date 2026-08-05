---
type: Rust Function
title: filetime_from_rfc3339_utc
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L340-L342
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_reminder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_client_submit_time_filetime
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_named_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_reminder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/task/reminder_delta_minutes
  - functions/crates/lpe-exchange/src/mapi/properties/tests/reminder_signal_time_wins_independently_of_property_order
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/public_folder_item_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/task_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/tests/appointment_fast_transfer_named_lid_includes_property_definition
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/recoverable_item_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_conversation_action_example_round_trips_fai_properties
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_outlook_inbox_view_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_inbox_query_rows_projects_sender_and_delivery_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/rfc3339_filetime_accepts_postgresql_microseconds_and_preserves_100ns_ticks
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/outlook_fai_copyto_generates_a_mapiuid_search_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_includes_special_folder_message_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_property_excludes_to_special_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_excludes_to_special_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_includes_to_special_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_respects_normal_and_fai_scope_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers
  - functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_contact_sync_versions
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_mixed_reminder_and_malformed_recurrence_has_no_side_effect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_set_properties_updates_canonical_mail_reminder_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_default_contacts_folder_properties_use_persisted_change_number
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_cached_mode_properties_include_canonical_change_keys
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable
---

# Signature

`pub(crate) fn filetime_from_rfc3339_utc(value: &str) -> u64`

# Calls

- [parse_rfc3339_utc_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime.md)

# Called by

- [conversation_action_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties.md)
- [normal_message_debug_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [rop_property_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction.md)
- [append_synchronization_import_message_change_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [conversation_action_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value.md)
- [event_property_value_with_optional_version](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [event_reminder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_reminder_property_value.md)
- [email_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [email_client_submit_time_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_client_submit_time_filetime.md)
- [note_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_property_value.md)
- [journal_entry_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value.md)
- [journal_entry_named_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_named_property_value.md)
- [task_property_value_with_reminder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)
- [task_reminder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_reminder_property_value.md)
- [reminder_delta_minutes](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/reminder_delta_minutes.md)
- [reminder_signal_time_wins_independently_of_property_order](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/reminder_signal_time_wins_independently_of_property_order.md)
- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [public_folder_item_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/public_folder_item_sync_object.md)
- [task_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/task_sync_object.md)
- [journal_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object.md)
- [calendar_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object.md)
- [appointment_fast_transfer_named_lid_includes_property_definition](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/appointment_fast_transfer_named_lid_includes_property_definition.md)
- [delegate_freebusy_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value.md)
- [serialize_message_row_with_table_instance](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [serialize_recoverable_item_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row.md)
- [recoverable_item_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/recoverable_item_property_value.md)
- [microsoft_conversation_action_example_round_trips_fai_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_conversation_action_example_round_trips_fai_properties.md)
- [normal_message_row_projects_outlook_inbox_view_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_outlook_inbox_view_columns.md)
- [normal_inbox_query_rows_projects_sender_and_delivery_time](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_inbox_query_rows_projects_sender_and_delivery_time.md)
- [write_special_message_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property.md)
- [rfc3339_filetime_accepts_postgresql_microseconds_and_preserves_100ns_ticks](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/rfc3339_filetime_accepts_postgresql_microseconds_and_preserves_100ns_ticks.md)
- [microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties.md)
- [microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes.md)
- [microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property.md)
- [microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers.md)
- [fast_transfer_copy_properties_filters_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties.md)
- [microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root.md)
- [outlook_fai_copyto_generates_a_mapiuid_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/outlook_fai_copyto_generates_a_mapiuid_search_key.md)
- [content_sync_manifest_includes_special_folder_message_objects](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_includes_special_folder_message_objects.md)
- [content_sync_manifest_starts_fai_message_before_item_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties.md)
- [fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given.md)
- [microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker.md)
- [content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag.md)
- [content_sync_manifest_applies_property_excludes_to_special_objects](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_property_excludes_to_special_objects.md)
- [content_sync_manifest_applies_string8_property_excludes_to_special_objects](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_excludes_to_special_objects.md)
- [content_sync_manifest_applies_string8_property_includes_to_special_objects](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_includes_to_special_objects.md)
- [content_sync_manifest_respects_normal_and_fai_scope_flags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_respects_normal_and_fai_scope_flags.md)
- [special_message_headers_and_final_cnsets_share_durable_change_numbers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers.md)
- [folder_local_commit_time_max](../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max.md)
- [with_contact_sync_versions](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_contact_sync_versions.md)
- [commit_mapi_associated_config_create_in_tx](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx.md)
- [commit_mapi_associated_config_update_in_tx](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx.md)
- [commit_mapi_imported_fai_identity_in_tx](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)
- [commit_mapi_navigation_shortcut_create_in_tx](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx.md)
- [commit_mapi_navigation_shortcut_update_in_tx](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx.md)
- [mapi_over_http_set_properties_updates_canonical_event_and_task_reminders](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders.md)
- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread.md)
- [mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids.md)
- [mapi_over_http_calendar_mixed_reminder_and_malformed_recurrence_has_no_side_effect](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_mixed_reminder_and_malformed_recurrence_has_no_side_effect.md)
- [mapi_over_http_set_properties_updates_canonical_mail_reminder_state](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_set_properties_updates_canonical_mail_reminder_state.md)
- [mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds.md)
- [mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification.md)
- [mapi_over_http_default_contacts_folder_properties_use_persisted_change_number](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_default_contacts_folder_properties_use_persisted_change_number.md)
- [mapi_over_http_cached_mode_properties_include_canonical_change_keys](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_cached_mode_properties_include_canonical_change_keys.md)
- [mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders.md)
- [mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable.md)