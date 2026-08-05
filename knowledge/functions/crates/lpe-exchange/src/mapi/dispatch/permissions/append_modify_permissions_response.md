---
type: Rust Function
title: append_modify_permissions_response
resource: crates/lpe-exchange/src/mapi/dispatch/permissions.rs#L46-L328
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_handle_index_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/permissions_for_folder
  - functions/crates/lpe-exchange/src/mapi/permissions/may_share_from_rights
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_permissions_rows
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids
  - functions/crates/lpe-exchange/src/mapi/permissions/access_from_rights
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_permission
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_collection_permission
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_folder_permission
  - functions/crates/lpe-exchange/src/mapi/permissions/rop_modify_permissions_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_permissions_dispatch_response
---

# Signature

`pub(super) async fn append_modify_permissions_response<S>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_handle_index_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_handle_index_error_response.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [permissions_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/permissions_for_folder.md)
- [may_share_from_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/may_share_from_rights.md)
- [modify_permissions_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_permissions_rows.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [fetch_mapi_identities_by_object_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids.md)
- [access_from_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/access_from_rights.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [set_mapi_calendar_permission](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_permission.md)
- [set_mapi_calendar_collection_permission](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_collection_permission.md)
- [set_mapi_folder_permission](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_folder_permission.md)
- [rop_modify_permissions_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rop_modify_permissions_response.md)

# Called by

- [append_permissions_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_permissions_dispatch_response.md)