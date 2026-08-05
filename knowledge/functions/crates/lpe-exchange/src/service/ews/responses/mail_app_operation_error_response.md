---
type: Rust Function
title: mail_app_operation_error_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L3-L16
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/install_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/disable_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/uninstall_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token
---

# Signature

`pub(in crate::service) fn mail_app_operation_error_response( operation: &str, error: &anyhow::Error, ) -> String`

# Calls

- [operation_error_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [install_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/install_app.md)
- [disable_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/disable_app.md)
- [uninstall_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/uninstall_app.md)
- [get_client_access_token](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token.md)