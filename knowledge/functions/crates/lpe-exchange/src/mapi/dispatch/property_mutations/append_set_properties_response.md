---
type: Rust Function
title: append_set_properties_response
resource: crates/lpe-exchange/src/mapi/dispatch/property_mutations.rs#L59-L408
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_set_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_values
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_setprops_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/stage_delegate_freebusy_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_problem_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/log_set_properties_default_folder_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_special_folder_aliases
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/clear_folder_profile_property_tombstones
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/persist_profile_folder_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/restore_requested_property_problem_tags
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response
---

# Signature

`pub(super) async fn append_set_properties_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) -> PropertyMutationFlow where S: ExchangeStore,`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [set_properties_probe_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request.md)
- [log_set_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_set_properties_specific_debug.md)
- [associated_config_message_for_mutation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation.md)
- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [format_debug_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_values.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [post_hierarchy_setprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract.md)
- [record_post_hierarchy_setprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_setprops_contract.md)
- [record_post_hierarchy_request_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract.md)
- [normalize_named_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [stage_virtual_conversation_action_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values.md)
- [stage_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values.md)
- [stage_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values.md)
- [stage_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values.md)
- [stage_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values.md)
- [stage_existing_navigation_shortcut_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_values.md)
- [set_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [stage_delegate_freebusy_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/stage_delegate_freebusy_property_values.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [folder_set_property_problems](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems.md)
- [rop_set_properties_problem_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_problem_response.md)
- [log_set_properties_default_folder_response_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/log_set_properties_default_folder_response_debug.md)
- [default_folder_entry_id_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases.md)
- [upsert_mapi_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_special_folder_aliases.md)
- [record_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias.md)
- [default_folder_identification_safe_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_values.md)
- [apply_mapi_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values.md)
- [clear_folder_profile_property_tombstones](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/clear_folder_profile_property_tombstones.md)
- [persist_profile_folder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/persist_profile_folder_property_values.md)
- [restore_requested_property_problem_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/restore_requested_property_problem_tags.md)
- [rop_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_response.md)

# Called by

- [append_property_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response.md)