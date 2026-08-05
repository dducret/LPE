---
type: Rust Function
title: get_events_queued_response
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L262-L293
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/notifications/queued_notification_event_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_streaming_events_queued_response
---

# Signature

`pub(in crate::service) fn get_events_queued_response( subscription_id: &str, previous_watermark: &str, events: &[EwsQueuedNotification], has_more: bool, ) -> String`

# Calls

- [queued_notification_event_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/queued_notification_event_xml.md)

# Called by

- [durable_events_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response.md)
- [get_streaming_events_queued_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_streaming_events_queued_response.md)