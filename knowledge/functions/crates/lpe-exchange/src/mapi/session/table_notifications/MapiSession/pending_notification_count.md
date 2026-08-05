---
type: Rust Method
title: pending_notification_count
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L78-L80
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending
---

# Signature

`pub(in crate::mapi) fn pending_notification_count(&self) -> usize`

# Called by

- [notification_wait_event_pending](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending.md)