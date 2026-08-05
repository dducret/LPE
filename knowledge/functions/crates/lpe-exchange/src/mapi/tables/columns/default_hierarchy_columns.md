---
type: Rust Function
title: default_hierarchy_columns
resource: crates/lpe-exchange/src/mapi/tables/columns.rs#L3-L24
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/hierarchy_table_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/bootstrap_query_rows_total_count_keeps_sync_issues_leaf_until_backed
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_root_depth_hierarchy_query_requires_snapshot_backed_contents
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/default_hierarchy_columns_cover_table_projection_contract
---

# Signature

`pub(in crate::mapi) fn default_hierarchy_columns() -> Vec<u32>`

# Called by

- [log_outlook_hierarchy_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response.md)
- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [hierarchy_table_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/hierarchy_table_object.md)
- [bootstrap_query_rows_total_count_keeps_sync_issues_leaf_until_backed](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/bootstrap_query_rows_total_count_keeps_sync_issues_leaf_until_backed.md)
- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [access_plan_root_depth_hierarchy_query_requires_snapshot_backed_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_root_depth_hierarchy_query_requires_snapshot_backed_contents.md)
- [access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [query_rows_response_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [default_hierarchy_columns_cover_table_projection_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/default_hierarchy_columns_cover_table_projection_contract.md)