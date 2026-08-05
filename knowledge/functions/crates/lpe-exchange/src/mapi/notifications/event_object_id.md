---
type: Rust Function
title: event_object_id
resource: crates/lpe-exchange/src/mapi/notifications.rs#L762-L767
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data
  - functions/crates/lpe-exchange/src/mapi/notifications/append_event_object_ids
---

# Signature

`fn event_object_id(event: &MapiNotificationEvent) -> u64`

# Called by

- [append_notification_data](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data.md)
- [append_event_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_event_object_ids.md)