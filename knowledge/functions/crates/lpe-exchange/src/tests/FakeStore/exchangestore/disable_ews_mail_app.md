---
type: Rust Method
title: disable_ews_mail_app
resource: crates/lpe-exchange/src/tests/mod.rs#L5676-L5701
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/disable_app
---

# Signature

`fn disable_ews_mail_app<'a>( &'a self, principal: &'a AccountPrincipal, app_id: &'a str, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsMailAppInstall>`

# Called by

- [disable_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/disable_app.md)