---
type: Rust Function
title: private_logon_request_handle
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L149-L159
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/logon_request_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_dispatch_response
---

# Signature

`pub(super) fn private_logon_request_handle( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, ) -> bool`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)

# Called by

- [append_set_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response.md)
- [append_get_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response.md)
- [logon_request_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/logon_request_handle.md)
- [append_receive_folder_table_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_dispatch_response.md)