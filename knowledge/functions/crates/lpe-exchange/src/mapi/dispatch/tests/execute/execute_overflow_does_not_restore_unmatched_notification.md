---
type: Rust Function
title: execute_overflow_does_not_restore_unmatched_notification
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L59-L88
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/test_mapi_session
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/restore_pending_notifications_after_execute_overflow
---

# Signature

`fn execute_overflow_does_not_restore_unmatched_notification()`

# Calls

- [test_mapi_session](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/test_mapi_session.md)
- [content](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [record_notification](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [remove](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [take_pending_notification_delivery_batch](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch.md)
- [restore_pending_notifications_after_execute_overflow](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/restore_pending_notifications_after_execute_overflow.md)