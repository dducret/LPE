---
type: Rust Function
title: role_for_folder_id
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L158-L191
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_windowable_mail_contents_folder
  - functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
---

# Signature

`pub(in crate::mapi) fn role_for_folder_id(folder_id: u64) -> Option<&'static str>`

# Called by

- [hidden_configuration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class.md)
- [debug_role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id.md)
- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [hard_delete_folder_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents.md)
- [hard_delete_mailbox_tree_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents.md)
- [advertised_special_folder_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class.md)
- [append_get_permissions_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response.md)
- [append_modify_permissions_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)
- [append_get_rules_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response.md)
- [append_modify_rules_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)
- [bounded_search_criteria_from_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [is_windowable_mail_contents_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_windowable_mail_contents_folder.md)
- [email_matches_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)