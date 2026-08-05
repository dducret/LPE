---
type: Rust Function
title: get_item_error_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L18-L35
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
---

# Signature

`pub(in crate::service) fn get_item_error_response(code: &str, message: &str) -> String`

# Called by

- [handle](../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)
- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)