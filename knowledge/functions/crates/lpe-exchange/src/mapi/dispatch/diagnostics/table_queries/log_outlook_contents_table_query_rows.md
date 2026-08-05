---
type: Rust Function
title: log_outlook_contents_table_query_rows
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L576-L746
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_window
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_for_principal
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_outlook_contents_table_query_rows( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], selected_named_property_context: &str, snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [is_outlook_folder_table_debug_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target.md)
- [effective_contents_table_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [restricted_associated_folder_message_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [folder_message_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [query_row_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count.md)
- [format_outlook_query_row_window](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_window.md)
- [query_forward_read](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)
- [format_outlook_query_row_values_for_principal](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_for_principal.md)
- [format_normal_message_query_row_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_inbox_associated_wire_row_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_inbox_view_descriptor_behavior_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [warn_outlook_view_handoff_table_invariants](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants.md)

# Called by

- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)