---
type: Rust Function
title: log_outlook_bootstrap_phase
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1215-L1246
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(super) fn log_outlook_bootstrap_phase( principal: &AccountPrincipal, phase: &str, rop_id: &str, folder_id: Option<u64>, associated: bool, table_total_row_count: Option<u32>, returned_row_count: Option<u32>, output_handle: Option<u32>, default_folder_ids: &str, )`

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_logon_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response.md)
- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)