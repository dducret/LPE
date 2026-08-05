---
type: Rust Function
title: delegate_error_response_message
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L252-L263
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates
---

# Signature

`pub(in crate::service) fn delegate_error_response_message(code: &str, message: &str) -> String`

# Called by

- [remove_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate.md)
- [mutate_ews_delegates](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates.md)