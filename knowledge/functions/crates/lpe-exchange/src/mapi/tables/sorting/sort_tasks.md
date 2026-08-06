---
type: Rust Function
title: sort_tasks
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L299-L335
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
  - functions/crates/lpe-exchange/src/mapi/tables/tests/task_default_view_sort_orders_by_due_date
  - functions/crates/lpe-exchange/src/mapi/tables/tests/task_default_view_sort_orders_by_start_date
---

# Signature

`pub(in crate::mapi) fn sort_tasks( rows: &mut [&crate::mapi_store::MapiTask], sort_orders: &[MapiSortOrder], )`

# Calls

- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [task_default_view_sort_orders_by_due_date](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/task_default_view_sort_orders_by_due_date.md)
- [task_default_view_sort_orders_by_start_date](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/task_default_view_sort_orders_by_start_date.md)