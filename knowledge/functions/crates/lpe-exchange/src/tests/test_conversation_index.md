---
type: Rust Function
title: test_conversation_index
resource: crates/lpe-exchange/src/tests/mod.rs#L15360-L15365
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_conversation_action_applies_to_future_matching_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_conversation_action_cross_store_keeps_local_message_in_place
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation
---

# Signature

`fn test_conversation_index(conversation_id: Uuid) -> Vec<u8>`

# Called by

- [mapi_over_http_conversation_action_applies_to_future_matching_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_conversation_action_applies_to_future_matching_message.md)
- [mapi_over_http_conversation_action_cross_store_keeps_local_message_in_place](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_conversation_action_cross_store_keeps_local_message_in_place.md)
- [mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation.md)