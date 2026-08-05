---
type: Rust Function
title: sort_notes
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L329-L355
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(in crate::mapi) fn sort_notes( rows: &mut [&crate::mapi_store::MapiNote], sort_orders: &[MapiSortOrder], )`

# Calls

- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)