---
type: Rust Method
title: delete_user_configuration
resource: crates/lpe-exchange/src/service/ews/user_configuration.rs#L111-L151
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_ews_user_configuration
  - functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn delete_user_configuration( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [parse_ews_user_configuration_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/user_configuration/parse_ews_user_configuration_key.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [delete_ews_user_configuration](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_ews_user_configuration.md)
- [simple_operation_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)