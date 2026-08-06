---
type: Rust Function
title: serialize_object_property
resource: crates/lpe-exchange/src/mapi/rop.rs#L1164-L1487
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_durable_identity
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_associated_message_row
  - functions/crates/lpe-exchange/src/mapi/rop/contact_properties/serialize_contact_object_property
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row
  - functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_note_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_journal_entry_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_conversation_action_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_navigation_shortcut_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_note_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_journal_entry_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_table_message_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_with_pending_properties
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property_row_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  - functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_getprops_returns_search_type_for_saved_search_definition
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_getprops_projects_saved_search_definition_metadata
  - functions/crates/lpe-exchange/src/mapi/rop/tests/ipm_subtree_ostid_read_prefers_session_client_write
---

# Signature

`pub(in crate::mapi) fn serialize_object_property( object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, tag: u32, ) -> Vec<u8>`

# Calls

- [serialize_logon_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row.md)
- [write_mapi_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [write_property_default](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [serialize_message_row_with_durable_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_durable_identity.md)
- [serialize_mapi_message_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_mapi_message_row.md)
- [search_folder_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)
- [serialize_message_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row.md)
- [serialize_pending_message_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row.md)
- [serialize_pending_associated_message_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_associated_message_row.md)
- [serialize_contact_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/contact_properties/serialize_contact_object_property.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [serialize_pending_contact_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_contact_row.md)
- [serialize_event_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property.md)
- [serialize_pending_event_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row.md)
- [serialize_pending_task_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row.md)
- [serialize_pending_note_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_note_row.md)
- [serialize_pending_journal_entry_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_journal_entry_row.md)
- [serialize_pending_conversation_action_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_conversation_action_row.md)
- [serialize_pending_navigation_shortcut_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_navigation_shortcut_row.md)
- [task_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id.md)
- [serialize_task_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row.md)
- [note_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [serialize_note_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_note_row.md)
- [journal_entry_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [serialize_journal_entry_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_journal_entry_row.md)
- [navigation_shortcut_table_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_table_message_for_id.md)
- [navigation_shortcut_with_pending_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_with_pending_properties.md)
- [serialize_navigation_shortcut_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row.md)
- [named_view_message_for_folder_and_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id.md)
- [serialize_common_view_named_view_row_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid.md)
- [associated_config_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [serialize_associated_config_row_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid.md)
- [conversation_action_table_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id.md)
- [serialize_conversation_action_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row.md)
- [delegate_freebusy_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_message_for_id.md)
- [serialize_freebusy_row_staged](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged.md)
- [recoverable_item_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_item_for_id.md)
- [serialize_recoverable_item_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row.md)
- [public_folder_item_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [serialize_public_folder_item_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row.md)
- [serialize_session_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [attachment_for_message](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message.md)
- [serialize_attachment_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row.md)
- [serialize_pending_attachment_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)
- [serialize_saved_attachment_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row.md)
- [folder_row_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [serialize_folder_row_with_context_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version.md)
- [folder_version](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)
- [collaboration_folder_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [serialize_collaboration_folder_row_with_context_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version.md)
- [associated_folder_message_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [serialize_special_folder_row_with_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version.md)

# Called by

- [serialize_object_property_row_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property_row_with_custom.md)
- [fallback_default_specific_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)
- [write_flagged_property_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row.md)
- [rop_get_properties_all_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [log_get_properties_specific_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [format_property_value_shapes_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug.md)
- [format_default_view_entry_id_decoding](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding.md)
- [size_limited_specific_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties.md)
- [folder_getprops_returns_search_type_for_saved_search_definition](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_getprops_returns_search_type_for_saved_search_definition.md)
- [folder_getprops_projects_saved_search_definition_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_getprops_projects_saved_search_definition_metadata.md)
- [ipm_subtree_ostid_read_prefers_session_client_write](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/ipm_subtree_ostid_read_prefers_session_client_write.md)