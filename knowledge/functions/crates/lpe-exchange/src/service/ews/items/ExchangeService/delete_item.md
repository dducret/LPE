---
type: Rust Method
title: delete_item
resource: crates/lpe-exchange/src/service/ews/items.rs#L1135-L1300
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_accessible_task
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/responses/delete_item_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn delete_item( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [validate_mutating_item_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [requested_item_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [delete_accessible_task](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_accessible_task.md)
- [attribute_value_after](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)
- [next](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [delete_item_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/delete_item_success_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)