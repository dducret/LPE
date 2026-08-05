---
type: Rust Method
title: get_app_manifests
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L9-L15
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_mail_app_manifests
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/get_app_manifests_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_app_manifests( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_ews_mail_app_manifests](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_mail_app_manifests.md)
- [get_app_manifests_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/get_app_manifests_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)