---
type: Rust Function
title: append_save_changes_message_route_response
resource: crates/lpe-exchange/src/mapi/dispatch/message_save.rs#L3-L1310
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_flags_are_supported
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/task/default_task_for_mapping
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_task
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/notes/default_note_for_mapping
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/notes/default_journal_entry_for_mapping
  - functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/message_save_generation
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/message_handle_generation
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_recipient_replacement
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_saved
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_trash_sync_artifact
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_associated_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_sync_metadata_only
  - functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_actions_to_new_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_message_mapi_identity
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response
---

# Signature

`pub(super) async fn append_save_changes_message_route_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, mapi_request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, created_emails: &mut Vec<JmapEmail>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [save_flags_are_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_flags_are_supported.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)
- [save_pending_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact.md)
- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [task_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi.md)
- [default_task_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/default_task_for_mapping.md)
- [create_accessible_task](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_task.md)
- [remember_created_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity.md)
- [upsert_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values_from_map.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [note_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_input_from_mapi.md)
- [default_note_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/default_note_for_mapping.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [journal_entry_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi.md)
- [default_journal_entry_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/default_journal_entry_for_mapping.md)
- [conversation_action_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties.md)
- [conversation_action_target_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox_id.md)
- [apply_conversation_action_to_existing_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages.md)
- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [message_save_generation](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/message_save_generation.md)
- [message_handle_generation](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/message_handle_generation.md)
- [apply_staged_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values.md)
- [apply_staged_message_recipient_replacement](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_recipient_replacement.md)
- [attachment_for_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [record_message_saved](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_saved.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [append_existing_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)
- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)
- [save_existing_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact.md)
- [append_pending_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response.md)
- [apply_canonical_public_folder_item_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [pending_message_is_trash_sync_artifact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_trash_sync_artifact.md)
- [transient_associated_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_associated_message_id.md)
- [pending_message_is_sync_metadata_only](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_sync_metadata_only.md)
- [jmap_import_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message.md)
- [imported_message_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key.md)
- [apply_conversation_actions_to_new_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_actions_to_new_message.md)
- [remember_created_message_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_message_mapi_identity.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [record_last_post_hierarchy_create_save_object_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context.md)

# Called by

- [append_message_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response.md)