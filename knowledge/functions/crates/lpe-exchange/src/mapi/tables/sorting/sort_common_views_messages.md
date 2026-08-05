---
type: Rust Function
title: sort_common_views_messages
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L16-L37
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_optional_mapi_values
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/common_views_message_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns
---

# Signature

`pub(in crate::mapi) fn sort_common_views_messages( rows: &mut [MapiCommonViewsMessage], sort_orders: &[MapiSortOrder], mailbox_guid: Uuid, )`

# Calls

- [compare_optional_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_optional_mapi_values.md)
- [common_views_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)
- [common_views_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/common_views_message_id.md)

# Called by

- [format_common_views_inbox_shortcut_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context.md)
- [format_common_views_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [captured_common_views_query_rows_flags_heterogeneous_missing_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns.md)