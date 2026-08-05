---
type: Rust Function
title: restore_pending_notifications_after_execute_overflow
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L212-L227
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_exceeds_max_rop_out
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification
---

# Signature

`pub(super) fn restore_pending_notifications_after_execute_overflow( session: &mut MapiSession, mut delivered_notification_events: VecDeque<MapiNotificationEvent>, response_rop_buffer: &[u8], max_rop_out: u32, )`

# Calls

- [execute_response_exceeds_max_rop_out](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_exceeds_max_rop_out.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [execute_overflow_restores_deliverable_notification_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch.md)
- [execute_overflow_does_not_restore_unmatched_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification.md)