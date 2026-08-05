---
type: Rust Function
title: format_inbox_associated_query_row_window
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L352-L386
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_debug_summaries_honor_table_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view
---

# Signature

`pub(super) fn format_inbox_associated_query_row_window( account_id: Uuid, position: usize, forward_read: bool, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [sort_debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows.md)
- [select_query_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)

# Called by

- [format_outlook_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_window.md)
- [associated_config_debug_summaries_honor_table_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_debug_summaries_honor_table_restriction.md)
- [inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view.md)