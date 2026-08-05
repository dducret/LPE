---
type: Rust Function
title: find_conversation_response
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L243-L291
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/conversations/conversation_last_delivery
  - functions/crates/lpe-exchange/src/service/ews/xml/ews_usize_attribute
  - functions/crates/lpe-exchange/src/service/ews/conversations/conversation_summary_xml
  - functions/crates/lpe-exchange/src/service/ews/xml/count_tag_occurrences
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/find_conversation
---

# Signature

`pub(in crate::service) fn find_conversation_response( emails: &[JmapEmail], request: &str, ) -> String`

# Calls

- [conversation_last_delivery](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/conversation_last_delivery.md)
- [ews_usize_attribute](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/ews_usize_attribute.md)
- [conversation_summary_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/conversation_summary_xml.md)
- [count_tag_occurrences](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/count_tag_occurrences.md)

# Called by

- [find_conversation](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/ExchangeService/find_conversation.md)