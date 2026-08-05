---
type: Rust Method
title: record_inbox_associated_exact_findrow
resource: crates/lpe-exchange/src/mapi/session.rs#L326-L331
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content
---

# Signature

`pub(in crate::mapi) fn record_inbox_associated_exact_findrow(&mut self, matched: bool)`

# Called by

- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [classifier_reports_first_missing_gate_after_fai_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows.md)
- [classifier_requires_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_find_row_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open.md)
- [classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress.md)
- [classifier_accepts_exact_ipm_configuration_findrow_gate](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate.md)
- [classifier_accepts_findrow_delivered_fai_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content.md)