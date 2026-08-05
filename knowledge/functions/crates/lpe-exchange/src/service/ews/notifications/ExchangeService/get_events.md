---
type: Rust Method
title: get_events
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L25-L44
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_subscription_id
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_events( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [notification_subscription_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_subscription_id.md)
- [notification_watermark](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark.md)
- [durable_events_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/durable_events_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)