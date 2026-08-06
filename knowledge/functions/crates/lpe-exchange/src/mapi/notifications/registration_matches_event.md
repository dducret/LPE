---
type: Rust Function
title: registration_matches_event
resource: crates/lpe-exchange/src/mapi/notifications.rs#L919-L943
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/notifications/notification_type_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target
---

# Signature

`pub(in crate::mapi) fn registration_matches_event( registration: &MapiNotificationRegistration, event: &MapiNotificationEvent, ) -> bool`

# Calls

- [notification_type_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_type_matches.md)

# Called by

- [take_pending_notification_delivery_batch](../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch.md)
- [has_notification_target](../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target.md)