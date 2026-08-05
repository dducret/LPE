---
type: Rust Function
title: strict_content_sync_transfer_from_response
resource: crates/lpe-exchange/src/tests/mod.rs#L13766-L13783
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_freebusy_data_folder_content_sync_projects_canonical_fai_messages
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_sync_projects_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxcfxics_4_3_2_partial_item_download_uses_full_item_fallback
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_client_state_controls_baseline_versus_delta_selection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_freebusy_data_sync_projects_postgresql_delegate_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_read_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_partial_item_uses_microsoft_full_item_fallback
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_only_specified_properties_limits_message_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_only_specified_body_returns_body_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_property_tags_exclude_message_properties_by_default
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_sync_suppresses_lpe_search_definition_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_root_adjacent_special_content_sync_uses_zero_length_state_sets
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_fai_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_conversation_action_sync_is_state_only
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_delete_does_not_allocate_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_inbox_fai_sync_exports_no_default_view
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_contacts_fai_sync_exports_no_default_view
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
---

# Signature

`fn strict_content_sync_transfer_from_response( response_rops: &[u8], ) -> Result<StrictContentSyncStream, String>`

# Calls

- [mapi_fast_transfer_chunks](../../../../../functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks.md)
- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)

# Called by

- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_freebusy_data_folder_content_sync_projects_canonical_fai_messages](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_freebusy_data_folder_content_sync_projects_canonical_fai_messages.md)
- [mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint.md)
- [mapi_over_http_advertised_calendar_sync_projects_default_collection_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_sync_projects_default_collection_event.md)
- [mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows.md)
- [mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration.md)
- [mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties.md)
- [mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection.md)
- [mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar.md)
- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [mapi_over_http_microsoft_oxcfxics_4_3_2_partial_item_download_uses_full_item_fallback](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxcfxics_4_3_2_partial_item_download_uses_full_item_fallback.md)
- [mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets.md)
- [mapi_over_http_ics_client_state_controls_baseline_versus_delta_selection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_client_state_controls_baseline_versus_delta_selection.md)
- [mapi_over_http_replays_outlook_contact_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save.md)
- [mapi_over_http_contacts_sync_exports_associated_config_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes.md)
- [mapi_over_http_contact_content_sync_exports_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes.md)
- [mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification.md)
- [mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_contacts_search_content_sync_uses_search_folder_parent](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent.md)
- [mapi_over_http_freebusy_data_sync_projects_postgresql_delegate_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_freebusy_data_sync_projects_postgresql_delegate_state.md)
- [mapi_over_http_public_folder_content_sync_exports_canonical_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_items.md)
- [mapi_over_http_public_folder_content_sync_exports_canonical_read_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_read_state.md)
- [mapi_over_http_content_sync_partial_item_uses_microsoft_full_item_fallback](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_partial_item_uses_microsoft_full_item_fallback.md)
- [mapi_over_http_content_sync_only_specified_properties_limits_message_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_only_specified_properties_limits_message_properties.md)
- [mapi_over_http_content_sync_only_specified_body_returns_body_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_only_specified_body_returns_body_property.md)
- [mapi_over_http_content_sync_property_tags_exclude_message_properties_by_default](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_property_tags_exclude_message_properties_by_default.md)
- [mapi_over_http_common_views_sync_suppresses_lpe_search_definition_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_sync_suppresses_lpe_search_definition_fai.md)
- [mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai.md)
- [mapi_over_http_empty_root_adjacent_special_content_sync_uses_zero_length_state_sets](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_root_adjacent_special_content_sync_uses_zero_length_state_sets.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity.md)
- [mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists.md)
- [mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save.md)
- [mapi_over_http_conversation_action_content_sync_exports_fai_rows](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_fai_rows.md)
- [mapi_over_http_empty_conversation_action_sync_is_state_only](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_conversation_action_sync_is_state_only.md)
- [mapi_over_http_conversation_action_content_sync_exports_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes.md)
- [mapi_over_http_associated_config_content_sync_exports_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_content_sync_exports_deletes.md)
- [mapi_over_http_associated_config_delete_does_not_allocate_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_delete_does_not_allocate_identity.md)
- [mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone.md)
- [mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone.md)
- [mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes.md)
- [mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding.md)
- [mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape.md)
- [mapi_over_http_content_sync_incremental_after_client_state_exports_delta](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta.md)
- [mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change.md)
- [mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state.md)
- [mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state.md)
- [mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc.md)
- [mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state.md)
- [mapi_over_http_sync_import_associated_message_persists_and_replays_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_empty_inbox_fai_sync_exports_no_default_view](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_inbox_fai_sync_exports_no_default_view.md)
- [mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset.md)
- [mapi_over_http_empty_contacts_fai_sync_exports_no_default_view](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_contacts_fai_sync_exports_no_default_view.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)