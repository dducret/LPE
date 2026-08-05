---
type: Rust Function
title: rop_parse_error_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L78-L80
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/read_next_execute_rop_request
---

# Signature

`pub(in crate::mapi) fn rop_parse_error_response() -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)

# Called by

- [parse_execute_rop_dispatch_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input.md)
- [read_next_execute_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/read_next_execute_rop_request.md)