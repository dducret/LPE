---
type: Rust Function
title: hierarchy_rows
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L15-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/sync_issues_hierarchy_table_is_leaf_until_backed
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_does_not_duplicate_sync_issues_children
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_search_hierarchy_row_belongs_to_search_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/persisted_sync_issues_roles_stay_leaf_in_startup_hierarchy
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder
  - functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants
  - functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders
---

# Signature

`pub(super) fn hierarchy_rows<'a>( folder_id: u64, mailboxes: &'a [JmapMailbox], snapshot: &'a MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, sort_orders: &[MapiSortOrder], mailbox_guid: Uuid, ) -> Vec<HierarchyRow<'a>>`

# Calls

- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)

# Called by

- [sync_issues_hierarchy_table_is_leaf_until_backed](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/sync_issues_hierarchy_table_is_leaf_until_backed.md)
- [ipm_subtree_hierarchy_does_not_duplicate_sync_issues_children](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_does_not_duplicate_sync_issues_children.md)
- [contacts_search_hierarchy_row_belongs_to_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_search_hierarchy_row_belongs_to_search_folder.md)
- [persisted_sync_issues_roles_stay_leaf_in_startup_hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/persisted_sync_issues_roles_stay_leaf_in_startup_hierarchy.md)
- [hierarchy_table_projects_user_saved_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_table_projects_user_saved_search_folder.md)
- [custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants.md)
- [ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders.md)