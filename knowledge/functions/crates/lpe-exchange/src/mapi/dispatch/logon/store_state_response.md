---
type: Rust Function
title: store_state_response
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L233-L239
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_store_state_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_store_state_response
---

# Signature

`pub(super) fn store_state_response(request: &RopRequest, has_input_handle: bool) -> Vec<u8>`

# Calls

- [rop_get_store_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_store_state_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_store_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_store_state_response.md)