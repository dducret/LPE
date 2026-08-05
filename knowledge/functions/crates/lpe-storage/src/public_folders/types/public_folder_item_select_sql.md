---
type: Rust Function
title: public_folder_item_select_sql
resource: crates/lpe-storage/src/public_folders/types.rs#L260-L290
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items_by_ids
---

# Signature

`pub(crate) fn public_folder_item_select_sql(where_clause: &str) -> String`

# Called by

- [fetch_public_folder_items](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items.md)
- [fetch_public_folder_items_by_ids](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items_by_ids.md)