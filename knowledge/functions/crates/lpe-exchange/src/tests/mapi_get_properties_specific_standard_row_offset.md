---
type: Rust Function
title: mapi_get_properties_specific_standard_row_offset
resource: crates/lpe-exchange/src/tests/mod.rs#L14942-L14959
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/calendar_change_key_from_get_properties_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_store_get_properties_specific_returns_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_specific_returns_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_specific_returns_collaboration_default_entry_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_inbox_get_properties_specific_returns_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_real_conversation_history_open_props_contents_and_notifications_succeed
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_default_contacts_folder_properties_use_persisted_change_number
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_attachment_initializes_documented_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxosrch_search_definition_message_properties_are_exposed
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/rule_organizer/mapi_over_http_exchange_rule_organizer_query_rows_opens_returned_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
---

# Signature

`fn mapi_get_properties_specific_standard_row_offset( bytes: &[u8], handle_index: u8, ) -> Result<usize, String>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread.md)
- [calendar_change_key_from_get_properties_response](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/calendar_change_key_from_get_properties_response.md)
- [mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local.md)
- [mapi_over_http_calendar_create_commits_event_and_attachment_together](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together.md)
- [mapi_over_http_store_get_properties_specific_returns_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_store_get_properties_specific_returns_calendar_default_entry_id.md)
- [mapi_over_http_root_get_properties_specific_returns_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_specific_returns_calendar_default_entry_id.md)
- [mapi_over_http_root_get_properties_specific_returns_collaboration_default_entry_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_specific_returns_collaboration_default_entry_ids.md)
- [mapi_over_http_inbox_get_properties_specific_returns_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_inbox_get_properties_specific_returns_calendar_default_entry_id.md)
- [mapi_over_http_real_conversation_history_open_props_contents_and_notifications_succeed](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_real_conversation_history_open_props_contents_and_notifications_succeed.md)
- [mapi_over_http_default_contacts_folder_properties_use_persisted_change_number](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_default_contacts_folder_properties_use_persisted_change_number.md)
- [mapi_over_http_microsoft_create_message_initializes_documented_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties.md)
- [mapi_over_http_pending_message_display_recipients_follow_modify_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients.md)
- [mapi_over_http_microsoft_create_attachment_initializes_documented_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_attachment_initializes_documented_properties.md)
- [mapi_over_http_microsoft_oxosrch_search_definition_message_properties_are_exposed](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxosrch_search_definition_message_properties_are_exposed.md)
- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_over_http_exchange_rule_organizer_query_rows_opens_returned_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/rule_organizer/mapi_over_http_exchange_rule_organizer_query_rows_opens_returned_message.md)
- [mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity.md)
- [mapi_over_http_sync_import_associated_message_persists_and_replays_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items.md)
- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)