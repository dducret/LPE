---
type: Rust Function
title: hierarchy_table_row_modified
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L196-L268
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_property_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants
---

# Signature

`pub(in crate::mapi) fn hierarchy_table_row_modified( table: &MapiObject, event: &MapiNotificationEvent, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Option<HierarchyTableRowModified>`

# Calls

- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [serialize_hierarchy_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_property_row.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants.md)