---
type: Rust Method
title: get_client_access_token
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L144-L182
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_id_from_request
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/requested_mail_app_token_scopes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/issue_ews_mail_app_token
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/get_client_access_token_response
  - functions/crates/lpe-exchange/src/service/ews/responses/mail_app_operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_client_access_token( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [mail_app_id_from_request](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/mail_app_id_from_request.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [requested_mail_app_token_scopes](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/requested_mail_app_token_scopes.md)
- [issue_ews_mail_app_token](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/issue_ews_mail_app_token.md)
- [get_client_access_token_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/get_client_access_token_response.md)
- [mail_app_operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/mail_app_operation_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)