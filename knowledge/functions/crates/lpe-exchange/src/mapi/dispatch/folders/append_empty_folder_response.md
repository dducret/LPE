---
type: Rust Function
title: append_empty_folder_response
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L175-L244
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/empty_folder_want_asynchronous
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/empty_folder_want_delete_associated
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/recoverable_items/hard_delete_recoverable_folder_contents
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/hard_delete_public_folder_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_dispatch/append_folder_dispatch_response
---

# Signature

`pub(super) async fn append_empty_folder_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [empty_folder_want_asynchronous](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/empty_folder_want_asynchronous.md)
- [empty_folder_want_delete_associated](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/empty_folder_want_delete_associated.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [recoverable_storage_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder.md)
- [hard_delete_recoverable_folder_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recoverable_items/hard_delete_recoverable_folder_contents.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [hard_delete_public_folder_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/hard_delete_public_folder_contents.md)
- [hard_delete_mailbox_tree_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents.md)
- [hard_delete_folder_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [rop_partial_completion_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response.md)

# Called by

- [append_folder_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_dispatch/append_folder_dispatch_response.md)