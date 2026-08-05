---
type: Rust Function
title: requested_mail_tips
resource: crates/lpe-exchange/src/service/ews/mail_tips.rs#L175-L191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_mail_tips
---

# Signature

`pub(in crate::service) fn requested_mail_tips(request: &str) -> HashSet<String>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)

# Called by

- [get_mail_tips](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_mail_tips.md)