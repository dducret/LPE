---
type: Rust Function
title: hierarchy_row_display_name
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L270-L280
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows
---

# Signature

`pub(super) fn hierarchy_row_display_name<'a>(row: &'a HierarchyRow<'a>) -> &'a str`

# Calls

- [special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata.md)

# Called by

- [sort_hierarchy_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/sort_hierarchy_rows.md)