---
type: Rust Method
title: record_default_view_normal_contents_table_query_rows
resource: crates/lpe-exchange/src/mapi/session.rs#L252-L271
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/transport/tests/records_default_view_normal_query_rows_without_marking_inbox_complete
---

# Signature

`pub(in crate::mapi) fn record_default_view_normal_contents_table_query_rows( &mut self, handle: Option<u32>, context: String, )`

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [classifier_requires_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open.md)
- [records_default_view_normal_query_rows_without_marking_inbox_complete](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/records_default_view_normal_query_rows_without_marking_inbox_complete.md)