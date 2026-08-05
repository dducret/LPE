---
type: Rust Function
title: effective_contents_table_columns
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L269-L286
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_navigation_shortcut_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_associated_config_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contents_columns
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_restrict
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_seek_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_query_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_find_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context
---

# Signature

`pub(super) fn effective_contents_table_columns( folder_id: u64, associated: bool, columns: &[u32], ) -> Vec<u32>`

# Calls

- [default_navigation_shortcut_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_navigation_shortcut_property_tags.md)
- [default_conversation_action_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags.md)
- [default_associated_config_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_associated_config_columns.md)
- [default_contents_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contents_columns.md)

# Called by

- [format_contents_table_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context.md)
- [log_outlook_contents_table_find_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)
- [log_outlook_contents_table_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_sort.md)
- [log_outlook_contents_table_restrict](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_restrict.md)
- [log_outlook_contents_table_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows.md)
- [log_outlook_contents_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response.md)
- [log_outlook_contents_table_seek_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_seek_row.md)
- [log_mapi_query_position_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)
- [format_inbox_associated_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_query_context.md)
- [format_inbox_associated_find_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_find_context.md)
- [format_common_views_inbox_shortcut_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context.md)