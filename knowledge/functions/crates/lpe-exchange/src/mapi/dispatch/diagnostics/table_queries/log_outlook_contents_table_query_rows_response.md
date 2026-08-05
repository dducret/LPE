---
type: Rust Function
title: log_outlook_contents_table_query_rows_response
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L748-L855
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_outlook_contents_table_query_rows_response( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, response: &[u8], snapshot: &MapiMailStoreSnapshot, selected_named_property_context: &str, queried_position: usize, )`

# Calls

- [is_outlook_folder_table_debug_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [effective_contents_table_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [query_forward_read](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)
- [format_inbox_associated_wire_row_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)
- [hex_preview](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [warn_outlook_view_handoff_table_invariants](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants.md)

# Called by

- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)