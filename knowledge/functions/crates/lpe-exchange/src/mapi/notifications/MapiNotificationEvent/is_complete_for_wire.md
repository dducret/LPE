---
type: Rust Method
title: is_complete_for_wire
resource: crates/lpe-exchange/src/mapi/notifications.rs#L231-L244
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target
---

# Signature

`pub(crate) fn is_complete_for_wire(&self) -> bool`

# Called by

- [rop_notify_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response.md)
- [take_pending_notification_delivery_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch.md)
- [has_notification_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target.md)