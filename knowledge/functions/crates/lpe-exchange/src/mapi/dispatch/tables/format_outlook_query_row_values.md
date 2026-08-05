---
type: Rust Function
title: format_outlook_query_row_values
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L449-L474
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_debug_summaries_honor_table_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_diagnostics_do_not_invent_named_views_for_wlink_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/quick_step_associated_debug_summaries_do_not_report_synthetic_custom_action_row
---

# Signature

`pub(super) fn format_outlook_query_row_values( account_id: Uuid, folder_id: u64, associated: bool, position: usize, forward_read: bool, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)

# Called by

- [associated_config_debug_summaries_honor_table_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_debug_summaries_honor_table_restriction.md)
- [inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view.md)
- [common_views_diagnostics_do_not_invent_named_views_for_wlink_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_diagnostics_do_not_invent_named_views_for_wlink_columns.md)
- [quick_step_associated_debug_summaries_do_not_report_synthetic_custom_action_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/quick_step_associated_debug_summaries_do_not_report_synthetic_custom_action_row.md)