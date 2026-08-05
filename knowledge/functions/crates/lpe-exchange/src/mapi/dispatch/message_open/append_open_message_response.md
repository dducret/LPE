---
type: Rust Function
title: append_open_message_response
resource: crates/lpe-exchange/src/mapi/dispatch/message_open.rs#L3-L527
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/open_message_folder_id
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_handle_generation
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_open_message_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/record_visible_inbox_message_open
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/unique_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/canonical_message_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/fallback_open_message_folder_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/common_view_named_view_message_for_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_opened
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/search_folder_definition_message_for_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/navigation_shortcut_message_for_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delegate_freebusy_message_for_open
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/conversation_action_message_for_open
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_identity_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_open
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/contacts/is_contact_link_timestamp_config
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_identity_matches_folder
  - functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response
---

# Signature

`pub(super) fn append_open_message_response( principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [open_message_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/open_message_folder_id.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [message_for_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id.md)
- [record_message_handle_generation](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_message_handle_generation.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_open_message_response_with_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients.md)
- [log_open_message_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/log_open_message_debug.md)
- [record_visible_inbox_message_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/record_visible_inbox_message_open.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [search_folder_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)
- [unique_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/unique_message_for_id.md)
- [canonical_message_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/canonical_message_folder_id.md)
- [fallback_open_message_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/fallback_open_message_folder_id.md)
- [contact_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id.md)
- [rop_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [task_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id.md)
- [note_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [journal_entry_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [common_view_named_view_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/common_view_named_view_message_for_open.md)
- [log_outlook_view_handoff](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_default_view_opened](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_opened.md)
- [search_folder_definition_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/search_folder_definition_message_for_open.md)
- [navigation_shortcut_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/navigation_shortcut_message_for_open.md)
- [delegate_freebusy_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delegate_freebusy_message_for_open.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [conversation_action_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/conversation_action_message_for_open.md)
- [conversation_action_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject.md)
- [associated_config_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [associated_config_message_for_identity_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_identity_id.md)
- [associated_config_message_for_folder_and_source_key_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key_id.md)
- [is_outlook_configuration_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class.md)
- [record_inbox_associated_config_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_open.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [rop_open_message_response_with_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties.md)
- [associated_config_named_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags.md)
- [is_contact_link_timestamp_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contacts/is_contact_link_timestamp_config.md)
- [associated_config_identity_matches_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_identity_matches_folder.md)
- [recoverable_storage_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder.md)
- [recoverable_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)

# Called by

- [append_message_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_dispatch/append_message_dispatch_response.md)