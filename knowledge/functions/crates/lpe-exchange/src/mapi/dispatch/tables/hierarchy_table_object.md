---
type: Rust Function
title: hierarchy_table_object
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L141-L163
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(super) fn hierarchy_table_object( folder_id: u64, depth: bool, depth_folder_ids: HashSet<u64>, deleted_advertised_special_folders: HashSet<u64>, ) -> MapiObject`

# Calls

- [default_hierarchy_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns.md)

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)