---
type: Rust Function
title: reserved_permission_rows
resource: crates/lpe-exchange/src/mapi/permissions.rs#L126-L141
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/permissions_for_folder
---

# Signature

`pub(crate) fn reserved_permission_rows(mailbox_id: Uuid) -> Vec<MapiFolderPermission>`

# Called by

- [permissions_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/permissions_for_folder.md)