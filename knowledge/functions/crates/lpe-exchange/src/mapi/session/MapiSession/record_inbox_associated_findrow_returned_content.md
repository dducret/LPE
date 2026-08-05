---
type: Rust Method
title: record_inbox_associated_findrow_returned_content
resource: crates/lpe-exchange/src/mapi/session.rs#L333-L336
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_detects_abandon_after_inbox_fai_query_rows_release
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned
---

# Signature

`pub(in crate::mapi) fn record_inbox_associated_findrow_returned_content(&mut self)`

# Called by

- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress.md)
- [classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress.md)
- [classifier_accepts_findrow_delivered_fai_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content.md)
- [session_detects_abandon_after_inbox_fai_query_rows_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_detects_abandon_after_inbox_fai_query_rows_release.md)
- [session_does_not_treat_findrow_delivered_fai_as_abandoned](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned.md)