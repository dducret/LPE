---
type: Rust Function
title: get_client_access_token_response
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L292-L326
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token
---

# Signature

`pub(in crate::service) fn get_client_access_token_response( event: &EwsMailAppTokenEvent, token: &str, scopes: &[String], ) -> String`

# Called by

- [get_client_access_token](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token.md)