---
type: Rust Function
title: requested_mailbox_emails
resource: crates/lpe-exchange/src/service/ews/mailboxes.rs#L44-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/search_mailboxes
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_hold_on_mailboxes
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/set_hold_on_mailboxes
---

# Signature

`pub(in crate::service) fn requested_mailbox_emails(request: &str) -> Vec<String>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [search_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/search_mailboxes.md)
- [get_hold_on_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_hold_on_mailboxes.md)
- [set_hold_on_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/set_hold_on_mailboxes.md)