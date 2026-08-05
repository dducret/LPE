---
type: Rust Method
title: get_phone_call_information
resource: crates/lpe-exchange/src/service/ews/unified_messaging.rs#L46-L70
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/phone_call_id_from_request
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_unified_messaging_call
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/phone_call_information_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_phone_call_information( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [phone_call_id_from_request](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/phone_call_id_from_request.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [fetch_ews_unified_messaging_call](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_unified_messaging_call.md)
- [phone_call_information_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/phone_call_information_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)