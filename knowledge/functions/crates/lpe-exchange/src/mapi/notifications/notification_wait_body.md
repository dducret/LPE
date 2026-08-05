---
type: Rust Function
title: notification_wait_body
resource: crates/lpe-exchange/src/mapi/notifications.rs#L628-L635
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response
---

# Signature

`pub(in crate::mapi) fn notification_wait_body(event_pending: bool) -> Vec<u8>`

# Called by

- [run_notification_wait](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait.md)
- [notification_wait_empty_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response.md)