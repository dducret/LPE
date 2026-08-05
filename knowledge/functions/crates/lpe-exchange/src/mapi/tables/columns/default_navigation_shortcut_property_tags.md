---
type: Rust Function
title: default_navigation_shortcut_property_tags
resource: crates/lpe-exchange/src/mapi/tables/columns.rs#L61-L98
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_diagnostics_do_not_invent_named_views_for_wlink_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/bootstrap_query_rows_total_count_keeps_empty_common_views_empty
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response
  - functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_default_columns_are_navigation_shortcut_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai
---

# Signature

`pub(in crate::mapi) fn default_navigation_shortcut_property_tags() -> Vec<u32>`

# Called by

- [effective_contents_table_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [common_views_diagnostics_do_not_invent_named_views_for_wlink_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_diagnostics_do_not_invent_named_views_for_wlink_columns.md)
- [bootstrap_query_rows_total_count_keeps_empty_common_views_empty](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/bootstrap_query_rows_total_count_keeps_empty_common_views_empty.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response.md)
- [query_rows_response_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [common_views_default_columns_are_navigation_shortcut_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_default_columns_are_navigation_shortcut_columns.md)
- [common_views_wlink_query_rows_do_not_add_named_views_without_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction.md)
- [common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai.md)