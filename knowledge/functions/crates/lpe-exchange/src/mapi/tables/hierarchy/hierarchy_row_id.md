---
type: Rust Function
title: hierarchy_row_id
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L441-L448
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_row_modified
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_depth_folder_ids_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/log_sync_issues_hierarchy_query_rows
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_is_present
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_search_hierarchy_row_belongs_to_search_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/persisted_sync_issues_roles_stay_leaf_in_startup_hierarchy
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders
---

# Signature

`pub(super) fn hierarchy_row_id(row: &HierarchyRow<'_>) -> u64`

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [hierarchy_table_row_modified](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_row_modified.md)
- [hierarchy_depth_folder_ids_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_depth_folder_ids_excluding_deleted.md)
- [sort_hierarchy_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows.md)
- [hierarchy_row_folder_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags.md)
- [log_sync_issues_hierarchy_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/log_sync_issues_hierarchy_query_rows.md)
- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)
- [hierarchy_row_property_is_present](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_is_present.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [contacts_search_hierarchy_row_belongs_to_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_search_hierarchy_row_belongs_to_search_folder.md)
- [persisted_sync_issues_roles_stay_leaf_in_startup_hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/persisted_sync_issues_roles_stay_leaf_in_startup_hierarchy.md)
- [hierarchy_table_projects_user_saved_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder.md)
- [hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection.md)
- [ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders.md)