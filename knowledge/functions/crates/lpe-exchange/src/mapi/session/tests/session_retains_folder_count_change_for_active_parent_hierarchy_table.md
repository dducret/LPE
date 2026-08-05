---
type: Rust Function
title: session_retains_folder_count_change_for_active_parent_hierarchy_table
resource: crates/lpe-exchange/src/mapi/session/tests.rs#L243-L306
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/tests/principal
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/remember_table_notification_eligibility
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch
---

# Signature

`fn session_retains_folder_count_change_for_active_parent_hierarchy_table()`

# Calls

- [principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/principal.md)
- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [remember_table_notification_eligibility](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/remember_table_notification_eligibility.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical.md)
- [with_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_parent_folder_id.md)
- [take_pending_notification_delivery_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch.md)