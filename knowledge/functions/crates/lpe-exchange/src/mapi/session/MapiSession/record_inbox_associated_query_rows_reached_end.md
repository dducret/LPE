---
type: Rust Method
title: record_inbox_associated_query_rows_reached_end
resource: crates/lpe-exchange/src/mapi/session.rs#L351-L359
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(in crate::mapi) fn record_inbox_associated_query_rows_reached_end( &mut self, context: String, )`

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)