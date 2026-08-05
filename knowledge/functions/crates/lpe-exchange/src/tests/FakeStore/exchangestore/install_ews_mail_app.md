---
type: Rust Method
title: install_ews_mail_app
resource: crates/lpe-exchange/src/tests/mod.rs#L5509-L5548
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/install_app
---

# Signature

`fn install_ews_mail_app<'a>( &'a self, principal: &'a AccountPrincipal, app_id: &'a str, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsMailAppInstall>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [install_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/install_app.md)