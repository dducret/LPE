---
type: Rust Function
title: get_permissions_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1327-L1329
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/permissions/rop_get_permissions_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response
---

# Signature

`pub(super) fn get_permissions_table_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [rop_get_permissions_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rop_get_permissions_table_response.md)

# Called by

- [append_get_permissions_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response.md)