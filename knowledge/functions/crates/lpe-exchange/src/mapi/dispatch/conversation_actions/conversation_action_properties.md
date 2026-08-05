---
type: Rust Function
title: conversation_action_properties
resource: crates/lpe-exchange/src/mapi/dispatch/conversation_actions.rs#L3-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_index_for_uuid
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(super) fn conversation_action_properties( action: &lpe_storage::ConversationAction, ) -> HashMap<u32, MapiValue>`

# Calls

- [conversation_index_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_index_for_uuid.md)
- [conversation_action_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)

# Called by

- [delete_conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties.md)
- [stage_virtual_conversation_action_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values.md)
- [stage_virtual_conversation_action_property_delete](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)