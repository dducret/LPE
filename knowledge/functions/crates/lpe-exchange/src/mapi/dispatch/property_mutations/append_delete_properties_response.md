---
type: Rust Function
title: append_delete_properties_response
resource: crates/lpe-exchange/src/mapi/dispatch/property_mutations.rs#L446-L648
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/delete_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/delete_canonical_message_text_properties
  - functions/crates/lpe-exchange/src/mapi/properties/delete_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/restore_requested_property_problem_tags
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_delete_properties_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_problem_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response
---

# Signature

`pub(super) async fn append_delete_properties_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [normalize_named_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [stage_virtual_conversation_action_property_delete](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete.md)
- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)
- [stage_existing_navigation_shortcut_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_deletions.md)
- [delete_conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/delete_conversation_action_properties.md)
- [delete_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [delete_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/delete_custom_property_values.md)
- [delete_canonical_message_text_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/delete_canonical_message_text_properties.md)
- [delete_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/delete_mapi_properties.md)
- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)
- [persisted_object_property_delete_is_idempotent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent.md)
- [mark_folder_profile_property_tombstones](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones.md)
- [restore_requested_property_problem_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/restore_requested_property_problem_tags.md)
- [rop_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_delete_properties_response.md)
- [rop_set_properties_problem_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_problem_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_property_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response.md)