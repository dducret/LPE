---
type: Rust Method
title: delete_ews_user_configuration
resource: crates/lpe-exchange/src/tests/mod.rs#L5064-L5085
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/delete_user_configuration
---

# Signature

`fn delete_ews_user_configuration<'a>( &'a self, account_id: Uuid, key: &'a EwsUserConfigurationKey, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, bool>`

# Called by

- [delete_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/delete_user_configuration.md)