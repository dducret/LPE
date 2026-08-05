---
type: Rust Function
title: default_contents_columns
resource: crates/lpe-exchange/src/mapi/tables/columns.rs#L26-L52
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_associated_config_columns
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/default_contents_columns_cover_table_projection_contract
---

# Signature

`pub(in crate::mapi) fn default_contents_columns() -> Vec<u32>`

# Called by

- [effective_contents_table_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [default_associated_config_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_associated_config_columns.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [query_rows_response_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [default_contents_columns_cover_table_projection_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/default_contents_columns_cover_table_projection_contract.md)