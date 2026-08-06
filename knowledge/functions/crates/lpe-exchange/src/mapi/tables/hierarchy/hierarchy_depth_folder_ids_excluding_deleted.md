---
type: Rust Function
title: hierarchy_depth_folder_ids_excluding_deleted
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L270-L289
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants
---

# Signature

`pub(in crate::mapi) fn hierarchy_depth_folder_ids_excluding_deleted( folder_id: u64, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, deleted_advertised_special_folders: &HashSet<u64>, ) -> HashSet<u64>`

# Calls

- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants.md)