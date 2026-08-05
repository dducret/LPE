---
type: Rust Method
title: record_inbox_normal_contents_table_setcolumns
resource: crates/lpe-exchange/src/mapi/session.rs#L176-L187
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_visible_inbox_release_before_query_rows
---

# Signature

`pub(in crate::mapi) fn record_inbox_normal_contents_table_setcolumns( &mut self, handle: Option<u32>, context: String, )`

# Called by

- [append_set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [classifier_reports_visible_inbox_release_before_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_visible_inbox_release_before_query_rows.md)