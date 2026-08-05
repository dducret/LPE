---
type: Rust Method
title: disconnect_phone_call
resource: crates/lpe-exchange/src/service/ews/unified_messaging.rs#L72-L104
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/phone_call_id_from_request
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/disconnect_ews_unified_messaging_call
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/disconnect_phone_call_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn disconnect_phone_call( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [phone_call_id_from_request](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/phone_call_id_from_request.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [disconnect_ews_unified_messaging_call](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/disconnect_ews_unified_messaging_call.md)
- [disconnect_phone_call_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/disconnect_phone_call_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)