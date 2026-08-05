---
type: Rust Function
title: notification_watermark_sequence
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L456-L458
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response
---

# Signature

`pub(in crate::service) fn notification_watermark_sequence(watermark: &str) -> Option<u64>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [durable_events_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response.md)
- [get_events_status_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response.md)