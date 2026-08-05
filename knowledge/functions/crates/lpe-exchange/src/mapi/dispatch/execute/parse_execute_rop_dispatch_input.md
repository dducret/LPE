---
type: Rust Function
title: parse_execute_rop_dispatch_input
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L281-L303
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_parse_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/session/read_handle_table
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn parse_execute_rop_dispatch_input( rop_buffer: &[u8], ) -> Result<(&[u8], Vec<u32>, bool), Vec<u8>>`

# Calls

- [rop_buffer_with_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response.md)
- [rop_parse_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_parse_error_response.md)
- [is_rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer.md)
- [read_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/read_handle_table.md)
- [rop_buffer_with_response_spec](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec.md)
- [rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)