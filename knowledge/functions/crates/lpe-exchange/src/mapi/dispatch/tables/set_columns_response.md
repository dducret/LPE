---
type: Rust Function
title: set_columns_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1380-L1382
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_columns_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
---

# Signature

`pub(super) fn set_columns_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [rop_set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_columns_response.md)

# Called by

- [append_set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)