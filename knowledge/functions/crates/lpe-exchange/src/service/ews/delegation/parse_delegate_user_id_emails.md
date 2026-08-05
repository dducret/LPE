---
type: Rust Function
title: parse_delegate_user_id_emails
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L432-L442
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/get_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate
---

# Signature

`pub(in crate::service) fn parse_delegate_user_id_emails(request: &str) -> Vec<String>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [normalize_trimmed_lowercase](../../../../../../../functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase.md)

# Called by

- [get_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/get_delegate.md)
- [remove_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate.md)