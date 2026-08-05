---
type: Rust Function
title: sort_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1384-L1386
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_sort_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
---

# Signature

`pub(super) fn sort_table_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [rop_sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_sort_table_response.md)

# Called by

- [append_sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)