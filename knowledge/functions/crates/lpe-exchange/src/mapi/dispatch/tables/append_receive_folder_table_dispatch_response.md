---
type: Rust Function
title: append_receive_folder_table_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1310-L1325
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_table_open_dispatch_response
---

# Signature

`pub(super) fn append_receive_folder_table_dispatch_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [private_logon_request_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle.md)
- [append_receive_folder_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_response.md)

# Called by

- [append_table_open_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_table_open_dispatch_response.md)