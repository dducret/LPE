---
type: Rust Function
title: append_get_permissions_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/permissions.rs#L10-L44
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_handle_index_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/permission_table_object
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_permissions_table_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_permissions_dispatch_response
---

# Signature

`pub(super) fn append_get_permissions_table_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_handle_index_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_handle_index_error_response.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [permission_table_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/permission_table_object.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [get_permissions_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_permissions_table_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_permissions_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_permissions_dispatch_response.md)