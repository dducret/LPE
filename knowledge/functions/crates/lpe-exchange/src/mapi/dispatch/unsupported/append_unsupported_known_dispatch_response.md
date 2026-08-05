---
type: Rust Function
title: append_unsupported_known_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/unsupported.rs#L3-L9
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/unsupported_known_rop_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn append_unsupported_known_dispatch_response( rop_id: RopId, request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [unsupported_known_rop_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/unsupported_known_rop_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)