---
type: Rust Function
title: queued_notification_event_xml
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L309-L337
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_events_queued_response
---

# Signature

`pub(in crate::service) fn queued_notification_event_xml( subscription_id: &str, event: &EwsQueuedNotification, ) -> String`

# Calls

- [notification_watermark](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark.md)

# Called by

- [get_events_queued_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_events_queued_response.md)