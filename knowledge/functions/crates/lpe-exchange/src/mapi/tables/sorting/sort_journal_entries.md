---
type: Rust Function
title: sort_journal_entries
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L383-L413
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/journal_entry_start_sort_key
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi/tables/tests/journal_default_view_sort_orders_by_log_start
---

# Signature

`pub(in crate::mapi) fn sort_journal_entries( rows: &mut [&crate::mapi_store::MapiJournalEntry], sort_orders: &[MapiSortOrder], )`

# Calls

- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [journal_entry_start_sort_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/journal_entry_start_sort_key.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [journal_default_view_sort_orders_by_log_start](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/journal_default_view_sort_orders_by_log_start.md)