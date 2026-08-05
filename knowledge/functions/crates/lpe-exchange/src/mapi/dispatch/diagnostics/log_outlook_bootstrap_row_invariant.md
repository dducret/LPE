---
type: Rust Function
title: log_outlook_bootstrap_row_invariant
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1248-L1288
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(super) fn log_outlook_bootstrap_row_invariant( principal: &AccountPrincipal, phase: &str, folder_id: u64, associated: bool, summary: &str, )`

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)