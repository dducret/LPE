---
type: Rust Function
title: abort_response
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L401-L407
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_abort_response
---

# Signature

`pub(super) fn abort_response(request: &RopRequest, input_object: Option<&MapiObject>) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_abort_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_abort_response.md)