---
type: Rust Function
title: unsupported_unknown_rop_response
resource: crates/lpe-exchange/src/mapi/dispatch/unsupported.rs#L22-L24
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/unsupported_rop_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/append_unsupported_unknown_dispatch_response
---

# Signature

`pub(super) fn unsupported_unknown_rop_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [unsupported_rop_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/unsupported_rop_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_unsupported_unknown_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/append_unsupported_unknown_dispatch_response.md)