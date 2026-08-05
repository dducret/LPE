---
type: Rust Function
title: append_move_copy_messages_response
resource: crates/lpe-exchange/src/mapi/dispatch/message_move_copy.rs#L3-L414
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_asynchronous
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy_raw
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_same_folder_move_partial_completion
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_move_to_deleted_items_partial_completion
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response
---

# Signature

`pub(super) async fn append_move_copy_messages_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [move_copy_want_asynchronous](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_asynchronous.md)
- [move_copy_want_copy_raw](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy_raw.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [move_copy_target_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [calendar_same_folder_move_partial_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_same_folder_move_partial_completion.md)
- [rop_partial_completion_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response.md)
- [calendar_move_to_deleted_items_partial_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_move_to_deleted_items_partial_completion.md)
- [move_copy_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids.md)
- [note_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [move_copy_want_copy](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy.md)
- [remember_created_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity.md)
- [journal_entry_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [recoverable_storage_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [recoverable_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)

# Called by

- [append_message_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response.md)