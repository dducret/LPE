---
type: Rust Function
title: operation_response_message
resource: crates/lpe-exchange/src/service/ews/responses.rs#L257-L274
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/conversations/get_conversation_items_response
---

# Signature

`pub(in crate::service) fn operation_response_message( operation: &str, code: &str, message: &str, ) -> String`

# Called by

- [get_conversation_items_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/conversations/get_conversation_items_response.md)