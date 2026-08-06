---
type: Rust Function
title: rop_get_properties_specific_response_with_custom
resource: crates/lpe-exchange/src/mapi/rop.rs#L69-L570
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/tables/folders/write_logon_property_row
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property_row_with_custom
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_associated_message_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_note_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_journal_entry_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_conversation_action_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_navigation_shortcut_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_table_message_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_with_pending_properties
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_search_folder_definition_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row
  - functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response
---

# Signature

`pub(in crate::mapi) fn rop_get_properties_specific_response_with_custom( request: &RopRequest, object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, custom_values: &HashMap<u32, Vec<u8>>, response_size_limit: usize, ) -> Vec<u8>`

# Calls

- [property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [unsupported_specific_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags.md)
- [size_limited_specific_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties.md)
- [log_get_properties_specific_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [write_logon_property_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/write_logon_property_row.md)
- [search_folder_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)
- [rop_error_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [input_handle_index](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [serialize_object_property_row_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property_row_with_custom.md)
- [serialize_pending_message_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row.md)
- [serialize_pending_associated_message_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_associated_message_row.md)
- [contact_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id.md)
- [serialize_pending_contact_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row.md)
- [event_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [serialize_pending_event_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row.md)
- [serialize_pending_task_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row.md)
- [serialize_pending_note_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_note_row.md)
- [serialize_pending_journal_entry_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_journal_entry_row.md)
- [serialize_pending_conversation_action_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_conversation_action_row.md)
- [serialize_pending_navigation_shortcut_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_navigation_shortcut_row.md)
- [task_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id.md)
- [note_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [journal_entry_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [navigation_shortcut_table_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_table_message_for_id.md)
- [navigation_shortcut_with_pending_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_with_pending_properties.md)
- [serialize_navigation_shortcut_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row.md)
- [named_view_message_for_folder_and_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id.md)
- [serialize_common_view_named_view_row_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid.md)
- [common_views_table_messages](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [serialize_search_folder_definition_row_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_search_folder_definition_row_with_mailbox_guid.md)
- [associated_config_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [serialize_associated_config_row_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid.md)
- [conversation_action_table_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id.md)
- [serialize_conversation_action_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row.md)
- [delegate_freebusy_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_message_for_id.md)
- [serialize_freebusy_row_staged](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged.md)
- [recoverable_item_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id.md)
- [serialize_recoverable_item_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row.md)
- [public_folder_item_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [serialize_session_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [attachment_for_message](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message.md)
- [serialize_pending_attachment_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)
- [serialize_saved_attachment_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row.md)
- [folder_row_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [serialize_folder_row_with_context_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version.md)
- [folder_version](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)
- [collaboration_folder_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [serialize_collaboration_folder_row_with_context_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version.md)
- [associated_folder_message_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [serialize_special_folder_row_with_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version.md)
- [get_properties_specific_typed_value_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag.md)
- [write_standard_property_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row.md)
- [write_flagged_property_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row.md)

# Called by

- [append_get_properties_specific_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [rop_get_properties_specific_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response.md)