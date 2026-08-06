---
type: Rust Method
title: folder_access_for_principal
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1534-L1562
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/permissions/access_from_rights
  - functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_special_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(crate) fn folder_access_for_principal( &self, folder_id: u64, principal_account_id: Uuid, ) -> Option<MapiFolderAccess>`

# Calls

- [access_from_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/access_from_rights.md)
- [rights_from_grant](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant.md)

# Called by

- [append_create_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)
- [append_delete_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [append_save_changes_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)
- [log_calendar_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_folder_contract.md)
- [log_special_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_special_folder_contract.md)
- [hard_delete_folder_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents.md)
- [hard_delete_mailbox_tree_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents.md)
- [append_create_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response.md)
- [append_set_message_read_flag_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)
- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [stage_existing_navigation_shortcut_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_values.md)
- [stage_existing_navigation_shortcut_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_deletions.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)