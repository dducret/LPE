---
type: Rust Method
title: get_user_configuration
resource: crates/lpe-exchange/src/service/ews/user_configuration.rs#L10-L37
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/get_user_configuration_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_user_configuration( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [parse_ews_user_configuration_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [fetch_ews_user_configuration](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_user_configuration.md)
- [get_user_configuration_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/get_user_configuration_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)