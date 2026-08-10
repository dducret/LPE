---
type: Rust Function
title: get_contents_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1263-L1265
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_contents_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(super) fn get_contents_table_response(request: &RopRequest, row_count: u32) -> Vec<u8>`

# Calls

- [rop_get_contents_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_contents_table_response.md)

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)