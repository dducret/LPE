---
type: Rust Function
title: execute_preserves_pending_table_notification_after_releasing_its_table
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L8-L60
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/test_mapi_session
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/remember_table_notification_eligibility
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/forget_table_notification_handle
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
---

# Signature

`fn execute_preserves_pending_table_notification_after_releasing_its_table()`

# Calls

- [test_mapi_session](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/test_mapi_session.md)
- [content](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [remember_table_notification_eligibility](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/remember_table_notification_eligibility.md)
- [record_notification](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [take_pending_notification_delivery_batch](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch.md)
- [forget_table_notification_handle](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/forget_table_notification_handle.md)
- [remove](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)