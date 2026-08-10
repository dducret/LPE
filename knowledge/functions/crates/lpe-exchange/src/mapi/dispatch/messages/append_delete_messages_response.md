---
type: Rust Function
title: append_delete_messages_response
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L955-L1232
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_messages_want_asynchronous
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_messages_notify_non_read
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/unsupported_rop_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_ids
  - functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_accessible_task
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_note
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_journal_entry
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_associated_config
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/mailbox_is_trash_or_descendant
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response
---

# Signature

`pub(super) async fn append_delete_messages_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [delete_messages_want_asynchronous](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_messages_want_asynchronous.md)
- [delete_messages_notify_non_read](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_messages_notify_non_read.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [unsupported_rop_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/unsupported_rop_response.md)
- [folder_access_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_ids.md)
- [recoverable_storage_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder.md)
- [recoverable_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id.md)
- [contact_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [task_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id.md)
- [delete_accessible_task](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_accessible_task.md)
- [note_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [delete_mapi_note](../../../../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_note.md)
- [record_sync_upload_content_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint.md)
- [journal_entry_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [delete_mapi_journal_entry](../../../../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_journal_entry.md)
- [conversation_action_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id.md)
- [navigation_shortcut_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id.md)
- [delete_mapi_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut.md)
- [associated_config_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [associated_config_message_for_folder_and_source_key_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id.md)
- [delete_mapi_associated_config](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_associated_config.md)
- [folder_local_default_named_view_is_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported.md)
- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [mailbox_is_trash_or_descendant](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/mailbox_is_trash_or_descendant.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [rop_partial_completion_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response.md)

# Called by

- [append_message_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response.md)