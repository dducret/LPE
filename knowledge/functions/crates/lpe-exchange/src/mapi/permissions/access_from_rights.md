---
type: Rust Function
title: access_from_rights
resource: crates/lpe-exchange/src/mapi/permissions.rs#L86-L92
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_access
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
---

# Signature

`pub(crate) fn access_from_rights(rights: u32) -> MapiFolderAccess`

# Called by

- [append_modify_permissions_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)
- [collaboration_folder_access](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_access.md)
- [folder_access_for_principal](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)