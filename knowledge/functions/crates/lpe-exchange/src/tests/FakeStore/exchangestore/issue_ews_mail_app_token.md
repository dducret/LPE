---
type: Rust Method
title: issue_ews_mail_app_token
resource: crates/lpe-exchange/src/tests/mod.rs#L5609-L5648
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token
---

# Signature

`fn issue_ews_mail_app_token<'a>( &'a self, principal: &'a AccountPrincipal, app_id: &'a str, _token_hash: &'a str, _scopes: &'a [String], _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsMailAppTokenEvent>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [get_client_access_token](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token.md)