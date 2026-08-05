---
type: Rust Function
title: conversation_summary_xml
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L413-L473
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response
---

# Signature

`fn conversation_summary_xml(thread_id: Uuid, messages: &[&JmapEmail]) -> String`

# Called by

- [find_conversation_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response.md)