---
type: Rust Function
title: stage_virtual_conversation_action_property_delete
resource: crates/lpe-exchange/src/mapi/dispatch/conversation_actions.rs#L262-L299
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/unadvertised_default_conversation_action_delete_properties_is_rejected
---

# Signature

`pub(super) fn stage_virtual_conversation_action_property_delete( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, property_tags: &[u32], ) -> Option<Result<()>>`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [is_outlook_default_conversation_action_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_default_conversation_action_id.md)
- [conversation_action_table_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id.md)
- [conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
- [unadvertised_default_conversation_action_delete_properties_is_rejected](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/unadvertised_default_conversation_action_delete_properties_is_rejected.md)