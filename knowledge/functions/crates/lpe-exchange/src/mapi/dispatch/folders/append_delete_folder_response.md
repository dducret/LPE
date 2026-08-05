---
type: Rust Function
title: append_delete_folder_response
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L246-L514
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_folder_flags
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_delete_uses_session_tombstone
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_delete_is_noop
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_deleted_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/forget_search_folder_definition
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition_was_deleted
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_dispatch/append_folder_dispatch_response
---

# Signature

`pub(super) async fn append_delete_folder_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [delete_folder_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_folder_flags.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [delete_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_folder_id.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [advertised_special_folder_delete_uses_session_tombstone](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_delete_uses_session_tombstone.md)
- [advertised_special_folder_delete_is_noop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_delete_is_noop.md)
- [rop_partial_completion_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response.md)
- [record_deleted_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_deleted_advertised_special_folder.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [search_folder_definition_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)
- [forget_search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/forget_search_folder_definition.md)
- [search_folder_definition_was_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition_was_deleted.md)

# Called by

- [append_folder_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_dispatch/append_folder_dispatch_response.md)