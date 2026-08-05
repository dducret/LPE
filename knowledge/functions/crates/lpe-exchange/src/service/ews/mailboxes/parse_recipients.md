---
type: Rust Function
title: parse_recipients
resource: crates/lpe-exchange/src/service/ews/mailboxes.rs#L9-L25
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail/parse_create_message_input
---

# Signature

`pub(in crate::service) fn parse_recipients( message: &str, collection_name: &str, ) -> Vec<SubmittedRecipientInput>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)

# Called by

- [parse_create_message_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/parse_create_message_input.md)