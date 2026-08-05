---
type: Rust Method
title: record_inbox_normal_contents_table
resource: crates/lpe-exchange/src/mapi/session.rs#L171-L174
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_visible_inbox_release_before_query_rows
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned
---

# Signature

`pub(in crate::mapi) fn record_inbox_normal_contents_table(&mut self)`

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [classifier_requires_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_find_row_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open.md)
- [classifier_reports_visible_inbox_release_before_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_visible_inbox_release_before_query_rows.md)
- [session_does_not_treat_findrow_delivered_fai_as_abandoned](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned.md)