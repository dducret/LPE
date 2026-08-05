---
type: Rust Function
title: append_create_folder_response
resource: crates/lpe-exchange/src/mapi/dispatch/folder_create.rs#L3-L564
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_root_hierarchy_folder
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_display_name
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_reserved
  - functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_special_folder_was_deleted
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_open_existing
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/private_create_folder_is_existing_response_flag
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_type
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folders
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/public_folder_handle_properties
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/user_saved_search_folder_definition_by_display_name
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/remember_search_folder_definition
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/create_folder_existing_mailbox_satisfies_deleted_advertised_request
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_dispatch/append_folder_dispatch_response
---

# Signature

`pub(super) async fn append_create_folder_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [is_root_hierarchy_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_root_hierarchy_folder.md)
- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)
- [create_folder_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_display_name.md)
- [create_folder_reserved](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_reserved.md)
- [advertised_special_folder_id_for_create](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create.md)
- [advertised_special_folder_was_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_special_folder_was_deleted.md)
- [create_folder_open_existing](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_open_existing.md)
- [private_create_folder_is_existing_response_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/private_create_folder_is_existing_response_flag.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_create_folder_response.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [create_folder_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_type.md)
- [public_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folders.md)
- [remember_created_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity.md)
- [public_folder_handle_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/public_folder_handle_properties.md)
- [user_saved_search_folder_definition_by_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/user_saved_search_folder_definition_by_display_name.md)
- [remember_search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/remember_search_folder_definition.md)
- [create_folder_existing_mailbox_satisfies_deleted_advertised_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/create_folder_existing_mailbox_satisfies_deleted_advertised_request.md)

# Called by

- [append_folder_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_dispatch/append_folder_dispatch_response.md)