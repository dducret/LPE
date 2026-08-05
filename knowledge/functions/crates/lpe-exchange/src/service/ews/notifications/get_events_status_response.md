---
type: Rust Function
title: get_events_status_response
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L339-L374
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark_folder_marker
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark_sequence
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_streaming_events_status_response
---

# Signature

`pub(in crate::service) fn get_events_status_response( subscription_id: &str, previous_watermark: &str, ) -> String`

# Calls

- [notification_watermark_folder_marker](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark_folder_marker.md)
- [notification_watermark_sequence](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark_sequence.md)
- [notification_watermark](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark.md)

# Called by

- [durable_events_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response.md)
- [get_streaming_events_status_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_streaming_events_status_response.md)