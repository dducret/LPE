---
type: Rust Function
title: get_streaming_events_queued_response
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L295-L307
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_events_queued_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response
---

# Signature

`pub(in crate::service) fn get_streaming_events_queued_response( subscription_id: &str, previous_watermark: &str, events: &[EwsQueuedNotification], has_more: bool, ) -> String`

# Calls

- [get_events_queued_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_events_queued_response.md)

# Called by

- [durable_events_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response.md)