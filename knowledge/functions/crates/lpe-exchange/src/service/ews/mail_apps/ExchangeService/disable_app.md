---
type: Rust Method
title: disable_app
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L76-L108
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_id_from_request
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/disable_ews_mail_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_state_response
  - functions/crates/lpe-exchange/src/service/ews/responses/mail_app_operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn disable_app( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [mail_app_id_from_request](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_id_from_request.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [disable_ews_mail_app](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/disable_ews_mail_app.md)
- [mail_app_state_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_state_response.md)
- [mail_app_operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/mail_app_operation_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)