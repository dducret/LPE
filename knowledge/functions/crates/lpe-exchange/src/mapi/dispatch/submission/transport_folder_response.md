---
type: Rust Function
title: transport_folder_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L121-L127
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_transport_folder_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_folder_response
---

# Signature

`pub(super) fn transport_folder_response(request: &RopRequest, has_input_object: bool) -> Vec<u8>`

# Calls

- [rop_get_transport_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_transport_folder_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_transport_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_folder_response.md)