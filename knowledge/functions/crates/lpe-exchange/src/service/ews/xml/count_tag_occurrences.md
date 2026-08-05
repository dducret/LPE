---
type: Rust Function
title: count_tag_occurrences
resource: crates/lpe-exchange/src/service/ews/xml.rs#L188-L190
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_payload_debug_detail
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
  - functions/crates/lpe-exchange/src/service/ews/xml/count_folder_elements
---

# Signature

`pub(in crate::service) fn count_tag_occurrences(value: &str, needle: &str) -> usize`

# Called by

- [find_conversation_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response.md)
- [ews_payload_debug_detail](../../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_payload_debug_detail.md)
- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
- [count_folder_elements](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/count_folder_elements.md)