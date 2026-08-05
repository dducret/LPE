---
type: Rust Function
title: execute_overflow_restores_deliverable_notification_batch
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L28-L56
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/test_mapi_session
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/restore_pending_notifications_after_execute_overflow
---

# Signature

`fn execute_overflow_restores_deliverable_notification_batch()`

# Calls

- [test_mapi_session](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/test_mapi_session.md)
- [content](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [record_notification](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [take_pending_notification_delivery_batch](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch.md)
- [restore_pending_notifications_after_execute_overflow](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/restore_pending_notifications_after_execute_overflow.md)