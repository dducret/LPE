---
type: Rust Function
title: delete_conversation_action_properties
resource: crates/lpe-exchange/src/mapi/dispatch/conversation_actions.rs#L183-L224
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
---

# Signature

`pub(super) async fn delete_conversation_action_properties<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, conversation_action_id: u64, snapshot: &MapiMailStoreSnapshot, property_tags: &[u32], mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> Result<()> where S: ExchangeStore,`

# Calls

- [conversation_action_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id.md)
- [conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [conversation_action_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties.md)
- [conversation_action_target_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox_id.md)
- [apply_conversation_action_to_existing_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages.md)

# Called by

- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)