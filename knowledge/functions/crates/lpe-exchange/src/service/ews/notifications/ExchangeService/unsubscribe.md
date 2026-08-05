---
type: Rust Method
title: unsubscribe
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L158-L169
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/unsubscribe_success_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn unsubscribe(&self, request: &str) -> Result<String>`

# Calls

- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [unsubscribe_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/unsubscribe_success_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)