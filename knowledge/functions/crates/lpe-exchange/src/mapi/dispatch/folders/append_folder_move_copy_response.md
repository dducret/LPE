---
type: Rust Function
title: append_folder_move_copy_response
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L516-L747
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_want_asynchronous
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_use_unicode
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_want_recursive
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_display_name
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy_move_or_copy
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/mailbox_parent_folder_id_for_dispatch
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_dispatch/append_folder_dispatch_response
---

# Signature

`pub(super) async fn append_folder_move_copy_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [folder_move_copy_want_asynchronous](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_want_asynchronous.md)
- [folder_move_copy_use_unicode](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_use_unicode.md)
- [folder_move_copy_want_recursive](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_want_recursive.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [move_copy_target_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [folder_move_copy_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_folder_id.md)
- [folder_move_copy_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/folder_move_copy_display_name.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [hierarchy_move_or_copy](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy_move_or_copy.md)
- [hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy.md)
- [rop_partial_completion_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [mailbox_parent_folder_id_for_dispatch](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/mailbox_parent_folder_id_for_dispatch.md)

# Called by

- [append_folder_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_dispatch/append_folder_dispatch_response.md)