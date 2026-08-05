---
type: Rust Function
title: log_outlook_contents_table_set_columns
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L376-L426
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_outlook_contents_table_set_columns( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, folder_id: u64, associated: bool, columns: &[u32], named_property_context: &str, snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [is_outlook_folder_table_debug_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_inbox_view_descriptor_set_columns_behavior_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract.md)
- [warn_outlook_view_handoff_table_invariants](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants.md)

# Called by

- [append_set_columns_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)