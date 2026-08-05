---
type: Rust Function
title: apply_execute_max_rop_out
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L170-L203
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_exceeds_max_rop_out
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_too_small_response
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_returns_buffer_too_small_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_preserves_extended_buffer_for_generic_overflow
---

# Signature

`pub(super) fn apply_execute_max_rop_out( request_id: &str, request_rop_buffer: &[u8], response_rop_buffer: Vec<u8>, max_rop_out: u32, ) -> Vec<u8>`

# Calls

- [execute_response_exceeds_max_rop_out](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_exceeds_max_rop_out.md)
- [rop_buffer_too_small_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_too_small_response.md)
- [is_rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer.md)
- [rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_max_rop_out_returns_buffer_too_small_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_returns_buffer_too_small_response.md)
- [execute_max_rop_out_preserves_extended_buffer_for_generic_overflow](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_max_rop_out_preserves_extended_buffer_for_generic_overflow.md)