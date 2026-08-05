---
type: Rust Function
title: is_root_hierarchy_folder
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L123-L128
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder
---

# Signature

`pub(in crate::mapi) fn is_root_hierarchy_folder(folder_id: u64) -> bool`

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [is_queryable_hierarchy_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_queryable_hierarchy_folder.md)