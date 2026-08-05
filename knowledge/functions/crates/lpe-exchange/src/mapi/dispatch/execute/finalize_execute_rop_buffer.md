---
type: Rust Function
title: finalize_execute_rop_buffer
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L350-L377
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_handle_table
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn finalize_execute_rop_buffer( responses: Vec<u8>, handle_slots: &[u32], output_handles: &[u32], response_handle_indexes: &[u8], echo_input_handle_table: bool, released_handle_indexes: &[u8], extended: bool, ) -> Vec<u8>`

# Calls

- [execute_response_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_handle_table.md)
- [rop_buffer_with_response_spec](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec.md)
- [rop_buffer_with_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response.md)
- [rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)