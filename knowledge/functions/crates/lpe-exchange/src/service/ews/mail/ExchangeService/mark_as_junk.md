---
type: Rust Method
title: mark_as_junk
resource: crates/lpe-exchange/src/service/ews/mail.rs#L104-L188
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys
  - functions/crates/lpe-exchange/src/service/ews/xml/ews_bool_attribute
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/mark_as_junk_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn mark_as_junk( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [validate_mutating_item_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [ews_bool_attribute](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/ews_bool_attribute.md)
- [requested_item_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [mark_as_junk_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/mark_as_junk_success_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)