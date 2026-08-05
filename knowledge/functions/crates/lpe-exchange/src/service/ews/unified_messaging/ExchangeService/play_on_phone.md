---
type: Rust Method
title: play_on_phone
resource: crates/lpe-exchange/src/service/ews/unified_messaging.rs#L8-L44
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_message_id_from_ews_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_ews_unified_messaging_call
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/play_on_phone_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn play_on_phone( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [requested_item_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [canonical_message_id_from_ews_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_message_id_from_ews_id.md)
- [create_ews_unified_messaging_call](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_ews_unified_messaging_call.md)
- [play_on_phone_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/play_on_phone_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)