---
type: Rust Function
title: get_streaming_events_status_response
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L376-L386
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response
---

# Signature

`pub(in crate::service) fn get_streaming_events_status_response( subscription_id: &str, previous_watermark: &str, ) -> String`

# Calls

- [get_events_status_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response.md)

# Called by

- [durable_events_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response.md)