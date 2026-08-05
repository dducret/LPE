---
type: Rust Method
title: get_reminders
resource: crates/lpe-exchange/src/service/ews/reminders.rs#L8-L21
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/reminders/get_reminders_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_reminders( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [get_reminders_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/reminders/get_reminders_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)