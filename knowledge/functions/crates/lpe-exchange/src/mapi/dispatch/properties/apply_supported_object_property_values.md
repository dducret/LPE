---
type: Rust Function
title: apply_supported_object_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L1351-L1546
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_object_property_values
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/contact/apply_canonical_contact_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_note_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_journal_entry_property_values
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map
  - functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_content_properties
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/delete_canonical_message_text_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_message_followup_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_all_message_followup_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(super) async fn apply_supported_object_property_values<S>( store: &S, principal: &AccountPrincipal, object: &MapiObject, values: Vec<(u32, MapiValue)>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [split_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_object_property_values.md)
- [folder_access_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [apply_canonical_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values.md)
- [apply_canonical_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/apply_canonical_contact_property_values.md)
- [apply_canonical_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values.md)
- [apply_canonical_task_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values.md)
- [apply_canonical_note_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_note_property_values.md)
- [apply_canonical_journal_entry_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_journal_entry_property_values.md)
- [conversation_action_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id.md)
- [conversation_action_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_properties.md)
- [apply_mapi_property_values_to_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map.md)
- [conversation_action_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties.md)
- [conversation_action_target_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox_id.md)
- [apply_conversation_action_to_existing_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/apply_conversation_action_to_existing_messages.md)
- [associated_config_message_for_mutation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation.md)
- [associated_config_mutation_base_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties.md)
- [associated_config_class_and_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject.md)
- [normalized_associated_config_content_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_content_properties.md)
- [upsert_mapi_associated_config](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config.md)
- [mapi_properties_to_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json.md)
- [apply_canonical_public_folder_item_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values.md)
- [custom_property_object_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity.md)
- [upsert_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values.md)

# Called by

- [apply_staged_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values.md)
- [delete_canonical_message_text_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/delete_canonical_message_text_properties.md)
- [copy_message_followup_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_message_followup_property_values_for_request.md)
- [copy_all_message_followup_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_all_message_followup_property_values_for_request.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)