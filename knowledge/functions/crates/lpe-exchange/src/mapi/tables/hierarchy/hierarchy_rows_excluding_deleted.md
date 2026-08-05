---
type: Rust Function
title: hierarchy_rows_excluding_deleted
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L35-L131
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folders
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/collaboration_folder_shadows_outlook_special_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/tests/deleted_advertised_quick_step_folder_unshadows_real_folder_in_hierarchy
---

# Signature

`pub(super) fn hierarchy_rows_excluding_deleted<'a>( folder_id: u64, mailboxes: &'a [JmapMailbox], snapshot: &'a MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, sort_orders: &[MapiSortOrder], mailbox_guid: Uuid, deleted_advertised_special_folders: &HashSet<u64>, ) -> Vec<HierarchyRow<'a>>`

# Calls

- [public_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folders.md)
- [restriction_matches_public_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder.md)
- [sort_hierarchy_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows.md)
- [mailbox_shadowed_by_active_outlook_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder.md)
- [mapi_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id.md)
- [restriction_matches_mailbox_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account.md)
- [collaboration_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders.md)
- [collaboration_folder_shadows_outlook_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/collaboration_folder_shadows_outlook_special_folder.md)
- [restriction_matches_collaboration_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder.md)
- [special_hierarchy_row_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [hierarchy_row_count_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted.md)
- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [deleted_advertised_quick_step_folder_unshadows_real_folder_in_hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/deleted_advertised_quick_step_folder_unshadows_real_folder_in_hierarchy.md)