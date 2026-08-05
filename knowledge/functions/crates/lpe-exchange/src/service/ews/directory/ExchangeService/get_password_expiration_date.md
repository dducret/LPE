---
type: Rust Method
title: get_password_expiration_date
resource: crates/lpe-exchange/src/service/ews/directory.rs#L72-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/responses/get_password_expiration_date_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_password_expiration_date( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [get_password_expiration_date_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/get_password_expiration_date_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)