---
type: Rust Function
title: format_contents_table_named_property_context
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties.rs#L61-L76
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/contents_table_named_property_context_reports_selected_columns
---

# Signature

`pub(in crate::mapi::dispatch) fn format_contents_table_named_property_context( session: &MapiSession, object: Option<&MapiObject>, ) -> String`

# Calls

- [effective_contents_table_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [format_debug_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)

# Called by

- [append_sort_table_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [append_restrict_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)
- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [append_find_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [append_table_control_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [contents_table_named_property_context_reports_selected_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/contents_table_named_property_context_reports_selected_columns.md)