---
type: Rust Function
title: test_session_for_outlook_startup
resource: crates/lpe-exchange/src/mapi/transport/tests.rs#L66-L68
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_visible_inbox_release_before_query_rows
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_inbox_contents_gate_after_receive_folder_verified
---

# Signature

`pub(in crate::mapi) fn test_session_for_outlook_startup() -> MapiSession`

# Called by

- [classifier_reports_first_missing_gate_after_fai_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows.md)
- [classifier_requires_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_find_row_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open.md)
- [classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress.md)
- [classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress.md)
- [classifier_reports_visible_inbox_release_before_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_visible_inbox_release_before_query_rows.md)
- [classifier_accepts_exact_ipm_configuration_findrow_gate](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate.md)
- [classifier_accepts_findrow_delivered_fai_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content.md)
- [classifier_reports_inbox_contents_gate_after_receive_folder_verified](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_inbox_contents_gate_after_receive_folder_verified.md)