---
type: Rust Function
title: spooler_advisory_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L285-L291
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_response
---

# Signature

`pub(super) fn spooler_advisory_response(request: &RopRequest, has_input_handle: bool) -> Vec<u8>`

# Calls

- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_spooler_advisory_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_response.md)