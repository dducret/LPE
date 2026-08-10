---
type: Rust Function
title: fast_transfer_manifest_for_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L1130-L1386
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_message_children
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/for_rop
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_top_folder_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_content_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/fast_transfer_message_content_buffer_with_special_object
  - functions/crates/lpe-exchange/src/mapi/sync/special_message_with_named_property_definitions
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_direct_fast_transfer_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id
  - functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id
  - functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id
  - functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_message_for_id
  - functions/crates/lpe-exchange/src/mapi/sync/delegate_freebusy_sync_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi/sync/public_folder_item_sync_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response
  - functions/crates/lpe-exchange/src/mapi/sync/tests/special_message_general_properties_follow_fast_transfer_property_filters
  - functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/sync/tests/local_freebusy_direct_copy_projects_account_scoped_entry_id
---

# Signature

`pub(in crate::mapi) fn fast_transfer_manifest_for_object( rop_id: u8, send_options: u8, level: u8, property_tags: &[u32], object: &MapiObject, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Option<(u64, Vec<u8>)>`

# Calls

- [fast_transfer_message_children](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_message_children.md)
- [for_rop](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/for_rop.md)
- [sync_mailboxes_for_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)
- [emails_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [sync_attachment_facts_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for.md)
- [fast_transfer_top_folder_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_top_folder_buffer_with_attachments.md)
- [folder_row_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [fast_transfer_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)
- [message_for_canonical_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/message_for_canonical_id.md)
- [fast_transfer_message_content_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_content_buffer_with_attachments.md)
- [associated_config_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [fast_transfer_message_content_buffer_with_special_object](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/fast_transfer_message_content_buffer_with_special_object.md)
- [special_message_with_named_property_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_message_with_named_property_definitions.md)
- [associated_config_direct_fast_transfer_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_direct_fast_transfer_object.md)
- [conversation_action_table_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id.md)
- [conversation_action_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object.md)
- [navigation_shortcut_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id.md)
- [navigation_shortcut_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object.md)
- [named_view_message_for_folder_and_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id.md)
- [common_view_named_view_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object.md)
- [delegate_freebusy_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_message_for_id.md)
- [delegate_freebusy_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/delegate_freebusy_sync_object.md)
- [public_folder_item_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [public_folder_item_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/public_folder_item_sync_object.md)

# Called by

- [append_fast_transfer_source_copy_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response.md)
- [special_message_general_properties_follow_fast_transfer_property_filters](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/special_message_general_properties_follow_fast_transfer_property_filters.md)
- [navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id.md)
- [fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder.md)
- [fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder.md)
- [local_freebusy_direct_copy_projects_account_scoped_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/local_freebusy_direct_copy_projects_account_scoped_entry_id.md)