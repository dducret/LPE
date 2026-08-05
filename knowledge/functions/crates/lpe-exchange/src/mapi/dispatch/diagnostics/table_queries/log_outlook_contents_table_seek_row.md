---
type: Rust Function
title: log_outlook_contents_table_seek_row
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L857-L939
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_outlook_contents_table_seek_row( principal: &AccountPrincipal, request: &RopRequest, object: Option<&MapiObject>, selected_named_property_context: &str, snapshot: &MapiMailStoreSnapshot, before_position: Option<usize>, response: &[u8], )`

# Calls

- [is_outlook_folder_table_debug_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target.md)
- [effective_contents_table_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [warn_outlook_view_handoff_table_invariants](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants.md)

# Called by

- [append_table_control_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)