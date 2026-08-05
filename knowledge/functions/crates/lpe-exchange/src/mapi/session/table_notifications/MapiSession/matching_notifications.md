---
type: Rust Method
title: matching_notifications
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L208-L216
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending
---

# Signature

`pub(in crate::mapi) fn matching_notifications( &self, events: Vec<MapiNotificationEvent>, ) -> Vec<MapiNotificationEvent>`

# Calls

- [has_notification_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target.md)

# Called by

- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [notification_wait_event_pending](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending.md)