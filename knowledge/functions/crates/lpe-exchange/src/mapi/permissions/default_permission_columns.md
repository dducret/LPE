---
type: Rust Function
title: default_permission_columns
resource: crates/lpe-exchange/src/mapi/permissions.rs#L23-L29
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/permission_table_object
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response
  - functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn default_permission_columns() -> Vec<u32>`

# Called by

- [permission_table_object](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/permission_table_object.md)
- [rop_query_columns_all_response](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response.md)
- [query_rows_response_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns.md)
- [rop_query_rows_response_inner](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)