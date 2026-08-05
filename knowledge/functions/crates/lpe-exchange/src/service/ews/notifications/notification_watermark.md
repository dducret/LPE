---
type: Rust Function
title: notification_watermark
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L429-L438
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_events
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_streaming_events
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription
  - functions/crates/lpe-exchange/src/service/ews/notifications/queued_notification_event_xml
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response
---

# Signature

`pub(in crate::service) fn notification_watermark( subscription_id: &str, folder_marker: Option<&str>, sequence: u64, ) -> String`

# Called by

- [get_events](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_events.md)
- [get_streaming_events](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_streaming_events.md)
- [register_pull_subscription](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription.md)
- [queued_notification_event_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/queued_notification_event_xml.md)
- [get_events_status_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response.md)