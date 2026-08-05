---
type: Rust Function
title: reset_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L442-L448
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reset_table_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_reset_table_response
---

# Signature

`pub(super) fn reset_table_response(request: &RopRequest, reset_succeeded: bool) -> Vec<u8>`

# Calls

- [rop_reset_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reset_table_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_reset_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_reset_table_response.md)