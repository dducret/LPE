---
type: Rust Function
title: log_outlook_contents_table_sort
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L428-L501
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_outlook_contents_table_sort( principal: &AccountPrincipal, request: &RopRequest, object: Option<&MapiObject>, selected_named_property_context: &str, snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [is_outlook_folder_table_debug_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target.md)
- [effective_contents_table_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [warn_outlook_view_handoff_table_invariants](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants.md)

# Called by

- [append_sort_table_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)