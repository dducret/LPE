---
type: Rust Function
title: permission_table_object
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L197-L204
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/permissions/default_permission_columns
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response
---

# Signature

`pub(super) fn permission_table_object(folder_id: u64) -> MapiObject`

# Calls

- [default_permission_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/default_permission_columns.md)

# Called by

- [append_get_permissions_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response.md)