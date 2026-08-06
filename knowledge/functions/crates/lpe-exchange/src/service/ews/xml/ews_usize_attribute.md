---
type: Rust Function
title: ews_usize_attribute
resource: crates/lpe-exchange/src/service/ews/xml.rs#L177-L179
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
---

# Signature

`pub(in crate::service) fn ews_usize_attribute(body: &str, tag: &str, attr: &str) -> Option<usize>`

# Calls

- [attribute_value_after](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)

# Called by

- [find_conversation_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response.md)
- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)