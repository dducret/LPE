---
type: Rust Function
title: append_unsupported_unknown_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/unsupported.rs#L11-L16
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/unsupported_unknown_rop_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn append_unsupported_unknown_dispatch_response( request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [unsupported_unknown_rop_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/unsupported_unknown_rop_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)