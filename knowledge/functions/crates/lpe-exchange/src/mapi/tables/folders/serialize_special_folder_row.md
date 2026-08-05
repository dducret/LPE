---
type: Rust Function
title: serialize_special_folder_row
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L250-L261
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version
  - functions/crates/lpe-exchange/src/mapi/tables/tests/special_folder_rows_use_global_counters_for_change_xids
  - functions/crates/lpe-exchange/src/mapi/tables/tests/special_folder_rows_project_deleted_count_total
  - functions/crates/lpe-exchange/src/mapi/tables/tests/quick_step_settings_is_projected_as_leaf_configuration_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/configuration_folders_project_hidden_attribute
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_row_projects_principal_ost_identity_when_available
  - functions/crates/lpe-exchange/src/mapi/tables/tests/root_and_ipm_subtree_rows_project_entry_id_identity
  - functions/crates/lpe-exchange/src/mapi/tables/tests/folder_type_rows_follow_microsoft_values
  - functions/crates/lpe-exchange/src/mapi/tables/tests/access_rows_follow_microsoft_flags
  - functions/crates/lpe-exchange/src/mapi/tables/tests/reminders_folder_projects_reminder_container_class
  - functions/crates/lpe-exchange/src/mapi/tables/tests/reminders_folder_projects_default_post_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/tests/configuration_special_folder_projects_default_post_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_row_projects_default_post_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ms_oxosfld_none_container_classes_serialize_as_empty_strings
---

# Signature

`pub(in crate::mapi) fn serialize_special_folder_row( folder_id: u64, mailboxes: &[JmapMailbox], columns: &[u32], principal: Option<&AccountPrincipal>, ) -> Vec<u8>`

# Calls

- [serialize_ipm_subtree_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row.md)
- [serialize_root_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row.md)
- [serialize_advertised_special_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row.md)

# Called by

- [serialize_special_folder_row_with_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version.md)
- [special_folder_rows_use_global_counters_for_change_xids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/special_folder_rows_use_global_counters_for_change_xids.md)
- [special_folder_rows_project_deleted_count_total](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/special_folder_rows_project_deleted_count_total.md)
- [quick_step_settings_is_projected_as_leaf_configuration_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/quick_step_settings_is_projected_as_leaf_configuration_folder.md)
- [configuration_folders_project_hidden_attribute](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/configuration_folders_project_hidden_attribute.md)
- [ipm_subtree_row_projects_principal_ost_identity_when_available](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_row_projects_principal_ost_identity_when_available.md)
- [root_and_ipm_subtree_rows_project_entry_id_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/root_and_ipm_subtree_rows_project_entry_id_identity.md)
- [folder_type_rows_follow_microsoft_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/folder_type_rows_follow_microsoft_values.md)
- [access_rows_follow_microsoft_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/access_rows_follow_microsoft_flags.md)
- [reminders_folder_projects_reminder_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/reminders_folder_projects_reminder_container_class.md)
- [reminders_folder_projects_default_post_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/reminders_folder_projects_default_post_message_class.md)
- [configuration_special_folder_projects_default_post_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/configuration_special_folder_projects_default_post_message_class.md)
- [ipm_subtree_row_projects_default_post_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_row_projects_default_post_message_class.md)
- [ms_oxosfld_none_container_classes_serialize_as_empty_strings](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ms_oxosfld_none_container_classes_serialize_as_empty_strings.md)