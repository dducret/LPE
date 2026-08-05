---
type: Rust Function
title: test_category_id
resource: crates/lpe-exchange/src/tests/mapi_over_http.rs#L64-L76
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_microsoft_categorized_table_collapse_state_restores_bookmark
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_categorized_table_sort_query_and_expand_rows
---

# Signature

`fn test_category_id(folder_id: u64, property_tag: u32, value: &str) -> u64`

# Called by

- [mapi_over_http_microsoft_categorized_table_collapse_state_restores_bookmark](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/recoverable_items/mapi_over_http_microsoft_categorized_table_collapse_state_restores_bookmark.md)
- [mapi_over_http_microsoft_categorized_table_sort_query_and_expand_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_microsoft_categorized_table_sort_query_and_expand_rows.md)