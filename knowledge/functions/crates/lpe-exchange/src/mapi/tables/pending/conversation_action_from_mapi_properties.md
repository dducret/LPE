---
type: Rust Function
title: conversation_action_from_mapi_properties
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L243-L299
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_id_from_index
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_text
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_conversation_action_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_conversation_action_example_round_trips_fai_properties
---

# Signature

`pub(in crate::mapi) fn conversation_action_from_mapi_properties( properties: &HashMap<u32, MapiValue>, ) -> lpe_storage::ConversationAction`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [conversation_id_from_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_id_from_index.md)
- [as_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_text.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [delete_conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [serialize_pending_conversation_action_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_conversation_action_row.md)
- [microsoft_conversation_action_example_round_trips_fai_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_conversation_action_example_round_trips_fai_properties.md)