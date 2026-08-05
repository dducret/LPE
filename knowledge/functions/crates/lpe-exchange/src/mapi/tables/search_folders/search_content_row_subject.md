---
type: Rust Function
title: search_content_row_subject
resource: crates/lpe-exchange/src/mapi/tables/search_folders.rs#L106-L111
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows
---

# Signature

`fn search_content_row_subject<'a>(row: &'a SearchContentRow<'a>) -> &'a str`

# Called by

- [sort_search_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/sort_search_content_rows.md)