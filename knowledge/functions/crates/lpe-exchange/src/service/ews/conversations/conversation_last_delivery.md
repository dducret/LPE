---
type: Rust Function
title: conversation_last_delivery
resource: crates/lpe-exchange/src/service/ews/conversations.rs#L495-L502
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response
---

# Signature

`fn conversation_last_delivery(emails: &[JmapEmail], thread_id: &Uuid) -> String`

# Called by

- [find_conversation_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/find_conversation_response.md)