---
type: Rust Function
title: special_sync_objects_for
resource: crates/lpe-exchange/src/mapi/sync.rs#L169-L360
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder
  - functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder
  - functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tasks_for_folder
  - functions/crates/lpe-exchange/src/mapi/sync/task_sync_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_items_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_search_results
  - functions/crates/lpe-exchange/src/mapi/sync/sync_object_projected_to_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_results
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_tasks
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/notes_for_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_size
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entries_for_folder
  - functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_messages
  - functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_sync_messages_for_folder
  - functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
  - functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync
  - functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key
  - functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_special_content_sync_advertises_appointment_objects
  - functions/crates/lpe-exchange/src/mapi/sync/tests/collaboration_default_views_are_not_synthetic_fai_sync_objects
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_uses_account_bound_entry_ids
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_does_not_emit_materialized_mail_header
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_group_header_sync_includes_group_identity_without_target
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties
---

# Signature

`pub(in crate::mapi) fn special_sync_objects_for( folder_id: u64, sync_type: u8, snapshot: &MapiMailStoreSnapshot, principal: &AccountPrincipal, ) -> Vec<mapi_mailstore::SpecialMessageSyncFact>`

# Calls

- [events_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder.md)
- [calendar_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object.md)
- [collaboration_folder_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [reminder_for_source](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source.md)
- [contacts_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder.md)
- [contact_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object.md)
- [tasks_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tasks_for_folder.md)
- [task_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/task_sync_object.md)
- [public_folder_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_items_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_items_for_folder.md)
- [contacts_search_results](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_search_results.md)
- [sync_object_projected_to_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_object_projected_to_folder.md)
- [todo_search_results](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_results.md)
- [reminder_tasks](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_tasks.md)
- [notes_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/notes_for_folder.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [note_size](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_size.md)
- [journal_entries_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entries_for_folder.md)
- [journal_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object.md)
- [common_views_sync_messages](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_messages.md)
- [common_views_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object.md)
- [conversation_action_table_messages](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_messages.md)
- [delegate_freebusy_messages](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages.md)
- [associated_config_sync_messages_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_sync_messages_for_folder.md)
- [populate_special_message_named_property_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)
- [empty_persisted_inbox_named_view_is_exported_by_fai_sync](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync.md)
- [calendar_fai_content_sync_preserves_imported_ics_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties.md)
- [outlook_inbox_fai_ics_omits_unsupported_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties.md)
- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)
- [associated_config_fai_no_foreign_identifiers_uses_local_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key.md)
- [calendar_special_content_sync_advertises_appointment_objects](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_special_content_sync_advertises_appointment_objects.md)
- [collaboration_default_views_are_not_synthetic_fai_sync_objects](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/collaboration_default_views_are_not_synthetic_fai_sync_objects.md)
- [common_views_shortcut_sync_uses_account_bound_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_uses_account_bound_entry_ids.md)
- [common_views_shortcut_sync_does_not_emit_materialized_mail_header](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_does_not_emit_materialized_mail_header.md)
- [common_views_group_header_sync_includes_group_identity_without_target](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_group_header_sync_includes_group_identity_without_target.md)
- [inbox_associated_content_sync_payload_emits_required_fai_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)