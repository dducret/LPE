---
type: Rust Function
title: get_password_expiration_date_error_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L332-L346
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_password_expiration_date
---

# Signature

`pub(in crate::service) fn get_password_expiration_date_error_response( code: &str, message: &str, ) -> String`

# Called by

- [get_password_expiration_date](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_password_expiration_date.md)