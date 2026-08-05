---
type: Rust Function
title: public_folder_select_sql
resource: crates/lpe-storage/src/public_folders/types.rs#L224-L258
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_children
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row
---

# Signature

`pub(crate) fn public_folder_select_sql(where_clause: &str) -> String`

# Called by

- [fetch_public_folder_children](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_children.md)
- [fetch_public_folder_row](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row.md)