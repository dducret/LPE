---
type: Rust Method
title: fetch_ews_user_configuration
resource: crates/lpe-exchange/src/tests/mod.rs#L5080-L5100
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/get_user_configuration
---

# Signature

`fn fetch_ews_user_configuration<'a>( &'a self, account_id: Uuid, key: &'a EwsUserConfigurationKey, ) -> StoreFuture<'a, Option<EwsUserConfiguration>>`

# Called by

- [get_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/get_user_configuration.md)