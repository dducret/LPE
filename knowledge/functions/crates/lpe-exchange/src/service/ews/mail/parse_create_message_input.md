---
type: Rust Function
title: parse_create_message_input
resource: crates/lpe-exchange/src/service/ews/mail.rs#L212-L260
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_recipients
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`pub(in crate::service) fn parse_create_message_input( principal: &AccountPrincipal, request: &str, ) -> Result<SubmitMessageInput>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [parse_recipients](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/parse_recipients.md)

# Called by

- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)