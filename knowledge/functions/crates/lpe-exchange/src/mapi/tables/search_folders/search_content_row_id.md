---
type: Rust Function
title: search_content_row_id
resource: crates/lpe-exchange/src/mapi/tables/search_folders.rs#L99-L104
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows
---

# Signature

`pub(super) fn search_content_row_id(row: &SearchContentRow<'_>) -> u64`

# Called by

- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [sort_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows.md)