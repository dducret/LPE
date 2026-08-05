---
type: Rust Function
title: address_types_response
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L173-L179
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_address_types_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_response
---

# Signature

`pub(super) fn address_types_response(request: &RopRequest, has_input_object: bool) -> Vec<u8>`

# Calls

- [rop_get_address_types_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_address_types_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_address_types_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_response.md)