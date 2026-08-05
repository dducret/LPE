---
type: Rust Function
title: filter_ignored_conversation_folders
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L393-L411
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items
---

# Signature

`pub(in crate::service) fn filter_ignored_conversation_folders( emails: &mut Vec<JmapEmail>, request: &str, )`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)

# Called by

- [get_conversation_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/get_conversation_items.md)