---
type: Rust Function
title: sort_search_content_rows
resource: crates/lpe-exchange/src/mapi/tables/search_folders.rs#L63-L97
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_subject
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_time
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_class
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_id
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(super) fn sort_search_content_rows( rows: &mut [SearchContentRow<'_>], sort_orders: &[MapiSortOrder], )`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [search_content_row_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_subject.md)
- [search_content_row_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_time.md)
- [search_content_row_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_class.md)
- [search_content_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_id.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)