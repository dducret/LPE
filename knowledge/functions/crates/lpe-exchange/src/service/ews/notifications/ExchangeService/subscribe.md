---
type: Rust Method
title: subscribe
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L8-L23
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription
  - functions/crates/lpe-exchange/src/service/ews/notifications/subscribe_success_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn subscribe( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_content](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [register_pull_subscription](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription.md)
- [subscribe_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/subscribe_success_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)