---
type: Rust Function
title: execute_response_exceeds_max_rop_out
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L205-L210
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/restore_pending_notifications_after_execute_overflow
---

# Signature

`pub(super) fn execute_response_exceeds_max_rop_out( response_rop_buffer: &[u8], max_rop_out: u32, ) -> bool`

# Called by

- [apply_execute_max_rop_out](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out.md)
- [restore_pending_notifications_after_execute_overflow](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/restore_pending_notifications_after_execute_overflow.md)