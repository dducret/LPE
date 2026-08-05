---
type: Rust Method
title: record_inbox_associated_contents_table
resource: crates/lpe-exchange/src/mapi/session.rs#L314-L317
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_detects_abandon_after_inbox_fai_query_rows_release
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned
---

# Signature

`pub(in crate::mapi) fn record_inbox_associated_contents_table(&mut self)`

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [classifier_reports_first_missing_gate_after_fai_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows.md)
- [classifier_requires_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_find_row_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open.md)
- [classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress.md)
- [classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress.md)
- [classifier_accepts_exact_ipm_configuration_findrow_gate](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate.md)
- [classifier_accepts_findrow_delivered_fai_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content.md)
- [session_detects_abandon_after_inbox_fai_query_rows_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_detects_abandon_after_inbox_fai_query_rows_release.md)
- [session_does_not_treat_findrow_delivered_fai_as_abandoned](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_does_not_treat_findrow_delivered_fai_as_abandoned.md)