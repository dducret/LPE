---
type: Rust Function
title: permission_member_id
resource: crates/lpe-exchange/src/mapi/permissions.rs#L143-L154
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/permissions/stable_text_member_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/permissions/serialize_permission_row
---

# Signature

`fn permission_member_id(permission: &MapiFolderPermission) -> u64`

# Calls

- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [stable_text_member_id](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/stable_text_member_id.md)

# Called by

- [serialize_permission_row](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/serialize_permission_row.md)