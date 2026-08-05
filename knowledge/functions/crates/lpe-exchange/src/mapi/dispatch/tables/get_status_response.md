---
type: Rust Function
title: get_status_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1401-L1403
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_get_status_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(super) fn get_status_response(request: &RopRequest, object: Option<&MapiObject>) -> Vec<u8>`

# Calls

- [rop_get_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_get_status_response.md)

# Called by

- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)