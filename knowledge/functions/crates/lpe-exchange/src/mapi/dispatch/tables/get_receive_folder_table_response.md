---
type: Rust Function
title: get_receive_folder_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1271-L1273
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_response
---

# Signature

`pub(super) fn get_receive_folder_table_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [rop_get_receive_folder_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response.md)

# Called by

- [append_receive_folder_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_response.md)