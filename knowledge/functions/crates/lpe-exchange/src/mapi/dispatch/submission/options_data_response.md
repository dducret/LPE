---
type: Rust Function
title: options_data_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L129-L135
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_options_data_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_options_data_response
---

# Signature

`pub(super) fn options_data_response(request: &RopRequest, has_input_object: bool) -> Vec<u8>`

# Calls

- [rop_options_data_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_options_data_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_options_data_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_options_data_response.md)