---
type: Rust Function
title: hierarchy_row_folder_flags
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L492-L524
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_folder_is_in_ipm_subtree
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
---

# Signature

`pub(super) fn hierarchy_row_folder_flags(row: &HierarchyRow<'_>, mailboxes: &[JmapMailbox]) -> u32`

# Calls

- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [hierarchy_folder_is_in_ipm_subtree](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_folder_is_in_ipm_subtree.md)
- [folder_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_type.md)

# Called by

- [hierarchy_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value.md)
- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)