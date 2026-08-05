---
type: Rust Function
title: automatic_fast_transfer_buffer_uses_execute_residual_output_budget
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L187-L213
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/available_execute_rop_response_size
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_transfer_size
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec
---

# Signature

`fn automatic_fast_transfer_buffer_uses_execute_residual_output_budget()`

# Calls

- [available_execute_rop_response_size](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/available_execute_rop_response_size.md)
- [fast_transfer_source_get_buffer_transfer_size](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_transfer_size.md)
- [rop_fast_transfer_source_get_buffer_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_fast_transfer_source_get_buffer_response.md)
- [rpc_header_ext_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)
- [rop_buffer_with_response_spec](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec.md)