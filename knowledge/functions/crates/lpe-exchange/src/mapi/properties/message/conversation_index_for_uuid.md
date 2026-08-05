---
type: Rust Function
title: conversation_index_for_uuid
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L264-L269
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_conversation_action_example_round_trips_fai_properties
---

# Signature

`pub(in crate::mapi) fn conversation_index_for_uuid(conversation_id: Uuid) -> Vec<u8>`

# Called by

- [conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties.md)
- [conversation_action_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value.md)
- [email_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [microsoft_conversation_action_example_round_trips_fai_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_conversation_action_example_round_trips_fai_properties.md)