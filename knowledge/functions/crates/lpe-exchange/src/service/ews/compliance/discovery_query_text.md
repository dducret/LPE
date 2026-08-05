---
type: Rust Function
title: discovery_query_text
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L278-L285
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/search_mailboxes
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/set_hold_on_mailboxes
---

# Signature

`pub(in crate::service) fn discovery_query_text(request: &str) -> String`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [search_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/search_mailboxes.md)
- [set_hold_on_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/set_hold_on_mailboxes.md)