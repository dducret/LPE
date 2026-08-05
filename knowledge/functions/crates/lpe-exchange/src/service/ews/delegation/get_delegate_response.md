---
type: Rust Function
title: get_delegate_response
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L226-L232
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/delegation/delegate_success_response_message
  - functions/crates/lpe-exchange/src/service/ews/delegation/delegate_operation_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/get_delegate
---

# Signature

`pub(in crate::service) fn get_delegate_response(delegates: &[EwsDelegate]) -> String`

# Calls

- [delegate_success_response_message](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/delegate_success_response_message.md)
- [delegate_operation_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/delegate_operation_response.md)

# Called by

- [get_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/get_delegate.md)