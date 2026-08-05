---
type: Rust Function
title: outlook_bootstrap_query_rows_phase
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L3-L44
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(in crate::mapi::dispatch) fn outlook_bootstrap_query_rows_phase( object: Option<&MapiObject>, ) -> Option<(&'static str, u64, bool)>`

# Called by

- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)