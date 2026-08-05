---
type: Rust Function
title: folder_row_for_id
resource: crates/lpe-exchange/src/mapi/rop.rs#L1604-L1612
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
---

# Signature

`pub(in crate::mapi) fn folder_row_for_id( folder_id: u64, mailboxes: &[JmapMailbox], ) -> Option<&JmapMailbox>`

# Calls

- [role_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)

# Called by

- [conversation_action_target_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/conversation_action_target_mailbox.md)
- [default_folder_hierarchy_projection_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_hierarchy_projection_for_debug.md)
- [debug_open_folder_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [append_create_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [append_open_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_delete_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)
- [append_folder_move_copy_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response.md)
- [folder_properties_for_open_from_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [append_move_copy_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)
- [append_save_changes_message_route_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [append_create_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response.md)
- [append_get_permissions_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response.md)
- [append_modify_permissions_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)
- [append_get_rules_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response.md)
- [append_modify_rules_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)
- [bounded_search_criteria_from_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)
- [append_submit_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)
- [append_synchronization_import_deletes_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [append_synchronization_import_message_move_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [serialize_session_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [format_folder_type_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)
- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [sync_mailboxes_for_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)
- [folder_message_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)