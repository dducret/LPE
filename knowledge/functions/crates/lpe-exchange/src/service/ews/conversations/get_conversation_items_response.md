---
type: Rust Function
title: get_conversation_items_response
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L293-L361
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_response_message
  - functions/crates/lpe-exchange/src/service/ews/conversations/conversation_node_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items
---

# Signature

`pub(in crate::service) fn get_conversation_items_response( emails: &[JmapEmail], conversation_ids: &[Uuid], request: &str, ) -> String`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [operation_response_message](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_response_message.md)
- [conversation_node_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/conversation_node_xml.md)

# Called by

- [get_conversation_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items.md)