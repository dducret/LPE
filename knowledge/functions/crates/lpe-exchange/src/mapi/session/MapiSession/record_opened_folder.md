---
type: Rust Method
title: record_opened_folder
resource: crates/lpe-exchange/src/mapi/session.rs#L107-L119
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content
  - functions/crates/lpe-exchange/src/mapi/transport/tests/required_default_folder_disconnect_coverage_reports_calendar_contacts_gap
---

# Signature

`pub(in crate::mapi) fn record_opened_folder(&mut self, folder_id: u64)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [classifier_reports_first_missing_gate_after_fai_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows.md)
- [classifier_requires_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_query_rows_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_find_row_after_inbox_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open.md)
- [classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress.md)
- [classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress.md)
- [classifier_accepts_exact_ipm_configuration_findrow_gate](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate.md)
- [classifier_accepts_findrow_delivered_fai_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content.md)
- [required_default_folder_disconnect_coverage_reports_calendar_contacts_gap](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/required_default_folder_disconnect_coverage_reports_calendar_contacts_gap.md)