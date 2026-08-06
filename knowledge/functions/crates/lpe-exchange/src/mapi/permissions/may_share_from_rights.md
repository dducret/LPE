---
type: Rust Function
title: may_share_from_rights
resource: crates/lpe-exchange/src/mapi/permissions.rs#L94-L96
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_access
---

# Signature

`pub(crate) fn may_share_from_rights(rights: u32) -> bool`

# Called by

- [append_modify_permissions_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)
- [collaboration_folder_access](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_access.md)