---
type: Rust Function
title: table_view_signature
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L418-L530
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/push_bytes
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/push_restriction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(in crate::mapi) fn table_view_signature( sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, ) -> u64`

# Calls

- [push_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/push_bytes.md)
- [push_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/push_restriction.md)

# Called by

- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)