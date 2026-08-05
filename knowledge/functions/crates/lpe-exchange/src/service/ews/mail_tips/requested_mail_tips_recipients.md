---
type: Rust Function
title: requested_mail_tips_recipients
resource: crates/lpe-exchange/src/service/ews/mail_tips.rs#L161-L173
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_mail_tips
---

# Signature

`pub(in crate::service) fn requested_mail_tips_recipients(request: &str) -> Vec<String>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)

# Called by

- [get_mail_tips](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_mail_tips.md)