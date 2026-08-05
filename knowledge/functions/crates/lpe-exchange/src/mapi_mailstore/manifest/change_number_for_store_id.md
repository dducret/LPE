---
type: Rust Function
title: change_number_for_store_id
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L326-L330
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/saved_associated_config_getprops_uses_same_batch_saved_message
  - functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/delegate_freebusy_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_version
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/public_folder_item_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/recoverable_item_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_rows_project_folder_id_and_last_modification_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_predecessor_change_list
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/store_id_change_numbers_use_global_counter
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/fallback_event_version
  - functions/crates/lpe-exchange/src/mapi_store/tests/test_mapi_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_read_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_import_move_to_deleted_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_move_uses_canonical_store
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_event_versions
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_accessible_event_to_deleted_items
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_update
---

# Signature

`pub(crate) fn change_number_for_store_id(store_id: u64) -> u64`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [append_save_changes_message_route_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [sync_mailboxes_with_collaboration_counts](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)
- [append_synchronization_import_message_change_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [collaboration_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)
- [public_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [common_view_named_view_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [conversation_action_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value.md)
- [event_property_value_with_optional_version](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [contact_property_value_with_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)
- [navigation_shortcut_property_value_with_store_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id.md)
- [note_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_property_value.md)
- [journal_entry_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value.md)
- [search_folder_definition_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value.md)
- [search_folder_definition_message_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [task_property_value_with_reminder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)
- [rop_get_receive_folder_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response.md)
- [saved_associated_config_getprops_uses_same_batch_saved_message](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/saved_associated_config_getprops_uses_same_batch_saved_message.md)
- [contact_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object.md)
- [navigation_shortcut_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object.md)
- [search_folder_definition_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object.md)
- [common_view_named_view_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object.md)
- [conversation_action_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object.md)
- [delegate_freebusy_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/delegate_freebusy_sync_object.md)
- [associated_config_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [delegate_freebusy_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value.md)
- [serialize_advertised_special_folder_row_with_counts](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts.md)
- [serialize_advertised_special_folder_row_with_counts_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_version.md)
- [serialize_root_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row.md)
- [serialize_ipm_subtree_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row.md)
- [special_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value.md)
- [serialize_public_folder_item_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row.md)
- [public_folder_item_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/public_folder_item_property_value.md)
- [serialize_recoverable_item_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row.md)
- [recoverable_item_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/recoverable_item_property_value.md)
- [inbox_associated_rows_project_folder_id_and_last_modification_time](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_rows_project_folder_id_and_last_modification_time.md)
- [default_folder_hierarchy_membership_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary.md)
- [hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)
- [virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [special_message_change_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_key.md)
- [special_message_predecessor_change_list](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_predecessor_change_list.md)
- [special_message_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_number.md)
- [store_id_change_numbers_use_global_counter](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/store_id_change_numbers_use_global_counter.md)
- [fallback_event_version](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/fallback_event_version.md)
- [test_mapi_event](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/test_mapi_event.md)
- [mapi_over_http_public_folder_content_sync_exports_canonical_read_state](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_read_state.md)
- [mapi_over_http_replays_outlook_calendar_sync_import_then_save](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save.md)
- [mapi_over_http_replays_outlook_calendar_import_move_to_deleted_items](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_import_move_to_deleted_items.md)
- [mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event.md)
- [calendar_sync_conflict_store](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_store.md)
- [mapi_over_http_sync_import_move_uses_canonical_store](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_move_uses_canonical_store.md)
- [fetch_mapi_event_versions](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_event_versions.md)
- [move_accessible_event_to_deleted_items](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/move_accessible_event_to_deleted_items.md)
- [commit_mapi_associated_config_update](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_update.md)