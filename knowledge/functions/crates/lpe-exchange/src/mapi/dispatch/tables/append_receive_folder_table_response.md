---
type: Rust Function
title: append_receive_folder_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1273-L1306
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_receive_folder_table_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_dispatch_response
---

# Signature

`pub(super) fn append_receive_folder_table_response( principal: &AccountPrincipal, session: &mut MapiSession, has_private_logon_handle: bool, request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [get_receive_folder_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_receive_folder_table_response.md)
- [record_receive_folder_verification_passed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed.md)

# Called by

- [append_receive_folder_table_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_receive_folder_table_dispatch_response.md)