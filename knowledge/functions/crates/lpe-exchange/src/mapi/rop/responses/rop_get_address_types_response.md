---
type: Rust Function
title: rop_get_address_types_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L278-L286
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/address_types_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_address_types_frame_boundary
---

# Signature

`pub(in crate::mapi) fn rop_get_address_types_response(request: &RopRequest) -> Vec<u8>`

# Called by

- [address_types_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/address_types_response.md)
- [execute_rop_response_summary_keeps_get_address_types_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_address_types_frame_boundary.md)