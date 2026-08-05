---
type: Rust Function
title: parse_delegate_meeting_delivery
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L321-L334
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/add_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/update_delegate
---

# Signature

`pub(in crate::service) fn parse_delegate_meeting_delivery( request: &str, ) -> Result<EwsDelegatePreferences>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [add_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/add_delegate.md)
- [update_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/update_delegate.md)