---
type: Rust Function
title: fast_transfer_source_get_buffer_transfer_size
resource: crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer.rs#L8-L23
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_buffer_size
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_uses_server_determined_buffer_size
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget
---

# Signature

`pub(super) fn fast_transfer_source_get_buffer_transfer_size( request: &RopRequest, residual_rop_out_size: usize, ) -> usize`

# Calls

- [fast_transfer_buffer_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_buffer_size.md)
- [fast_transfer_uses_server_determined_buffer_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_uses_server_determined_buffer_size.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [automatic_fast_transfer_buffer_uses_execute_residual_output_budget](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget.md)