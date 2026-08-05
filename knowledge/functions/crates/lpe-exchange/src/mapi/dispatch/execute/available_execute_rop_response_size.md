---
type: Rust Function
title: available_execute_rop_response_size
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L229-L244
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget
---

# Signature

`pub(super) fn available_execute_rop_response_size( max_rop_out: u32, extended: bool, preceding_response_size: usize, response_handle_count: usize, ) -> usize`

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)
- [automatic_fast_transfer_buffer_uses_execute_residual_output_budget](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget.md)