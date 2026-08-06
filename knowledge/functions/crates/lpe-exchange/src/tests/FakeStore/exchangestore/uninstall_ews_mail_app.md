---
type: Rust Method
title: uninstall_ews_mail_app
resource: crates/lpe-exchange/src/tests/mod.rs#L5707-L5737
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/uninstall_app
---

# Signature

`fn uninstall_ews_mail_app<'a>( &'a self, principal: &'a AccountPrincipal, app_id: &'a str, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsMailAppInstall>`

# Called by

- [uninstall_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/uninstall_app.md)