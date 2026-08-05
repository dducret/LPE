---
type: Rust Function
title: requested_conversation_ids
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L363-L368
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items
---

# Signature

`pub(in crate::service) fn requested_conversation_ids(request: &str) -> Vec<Uuid>`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)

# Called by

- [get_conversation_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items.md)