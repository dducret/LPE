---
type: Rust Method
title: upsert_ews_user_configuration
resource: crates/lpe-exchange/src/tests/mod.rs#L5097-L5131
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/create_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/update_user_configuration
---

# Signature

`fn upsert_ews_user_configuration<'a>( &'a self, input: UpsertEwsUserConfigurationInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsUserConfiguration>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [create_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/create_user_configuration.md)
- [update_user_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/ExchangeService/update_user_configuration.md)