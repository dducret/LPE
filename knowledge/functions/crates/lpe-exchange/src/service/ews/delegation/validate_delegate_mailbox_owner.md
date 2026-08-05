---
type: Rust Function
title: validate_delegate_mailbox_owner
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L302-L319
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/add_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/update_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/get_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate
---

# Signature

`pub(in crate::service) fn validate_delegate_mailbox_owner( principal: &AccountPrincipal, request: &str, ) -> Result<()>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [add_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/add_delegate.md)
- [update_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/update_delegate.md)
- [get_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/get_delegate.md)
- [remove_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate.md)