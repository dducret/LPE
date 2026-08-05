---
type: Rust Method
title: record_inbox_normal_contents_table_query_rows
resource: crates/lpe-exchange/src/mapi/session.rs#L189-L200
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open
---

# Signature

`pub(in crate::mapi) fn record_inbox_normal_contents_table_query_rows( &mut self, handle: Option<u32>, context: String, )`

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [classifier_accepts_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open.md)