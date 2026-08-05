---
type: Rust Method
title: conversation_source_emails
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L188-L232
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/find_conversation
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items
---

# Signature

`async fn conversation_source_emails( &self, principal: &AccountPrincipal, request: &str, ) -> Result<Vec<JmapEmail>>`

# Calls

- [element_content](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)

# Called by

- [find_conversation](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/find_conversation.md)
- [get_conversation_items](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items.md)