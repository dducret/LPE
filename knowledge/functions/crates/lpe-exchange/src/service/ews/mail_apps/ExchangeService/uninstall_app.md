---
type: Rust Method
title: uninstall_app
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L110-L142
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_id_from_request
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/uninstall_ews_mail_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_state_response
  - functions/crates/lpe-exchange/src/service/ews/responses/mail_app_operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn uninstall_app( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [mail_app_id_from_request](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_id_from_request.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [uninstall_ews_mail_app](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/uninstall_ews_mail_app.md)
- [mail_app_state_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_state_response.md)
- [mail_app_operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/mail_app_operation_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)