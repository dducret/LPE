---
type: Rust Function
title: format_outlook_query_row_window
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L311-L350
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_inbox_associated_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_diagnostics_do_not_invent_named_views_for_wlink_columns
---

# Signature

`pub(super) fn format_outlook_query_row_window( folder_id: u64, associated: bool, position: usize, forward_read: bool, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, columns: &[u32], account_id: Uuid, snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [format_inbox_associated_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_inbox_associated_query_row_window.md)
- [format_common_views_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window.md)

# Called by

- [log_outlook_contents_table_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows.md)
- [common_views_diagnostics_do_not_invent_named_views_for_wlink_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_diagnostics_do_not_invent_named_views_for_wlink_columns.md)