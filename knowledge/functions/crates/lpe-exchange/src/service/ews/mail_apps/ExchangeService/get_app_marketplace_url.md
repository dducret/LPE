---
type: Rust Method
title: get_app_marketplace_url
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L17-L40
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_app_marketplace_policy
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/get_app_marketplace_url_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_app_marketplace_url( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_ews_app_marketplace_policy](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_app_marketplace_policy.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [get_app_marketplace_url_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/get_app_marketplace_url_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)