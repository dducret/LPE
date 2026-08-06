---
type: Rust Function
title: table_changed_event
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L265-L269
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target
---

# Signature

`fn table_changed_event(event: &MapiNotificationEvent) -> MapiNotificationEvent`

# Called by

- [take_pending_notification_delivery_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch.md)
- [has_notification_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target.md)