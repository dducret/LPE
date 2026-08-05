---
type: Rust Function
title: conversation_action_size
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L396-L414
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value
---

# Signature

`pub(in crate::mapi) fn conversation_action_size(action: &lpe_storage::ConversationAction) -> usize`

# Calls

- [conversation_action_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject.md)

# Called by

- [conversation_action_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value.md)