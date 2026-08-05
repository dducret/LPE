---
type: Rust Function
title: delegate_operation_response
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L211-L224
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates
  - functions/crates/lpe-exchange/src/service/ews/delegation/get_delegate_response
---

# Signature

`pub(in crate::service) fn delegate_operation_response( operation: &str, response_messages: &str, ) -> String`

# Called by

- [remove_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate.md)
- [mutate_ews_delegates](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates.md)
- [get_delegate_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/get_delegate_response.md)