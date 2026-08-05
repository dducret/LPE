---
type: Rust Function
title: append_set_receive_folder_response
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L48-L109
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/set_receive_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/set_receive_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/valid_receive_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_id_for_message_class
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_receive_folder_dispatch_response
---

# Signature

`pub(super) fn append_set_receive_folder_response( principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [private_logon_request_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [set_receive_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/set_receive_folder_id.md)
- [set_receive_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/set_receive_folder_message_class.md)
- [valid_receive_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/valid_receive_folder_message_class.md)
- [receive_folder_id_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_id_for_message_class.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)

# Called by

- [append_receive_folder_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_receive_folder_dispatch_response.md)