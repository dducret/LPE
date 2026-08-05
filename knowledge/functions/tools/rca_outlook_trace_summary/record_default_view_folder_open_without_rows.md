---
type: Python Function
title: record_default_view_folder_open_without_rows
resource: tools/rca_outlook_trace_summary.py#L1538-L1587
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/default_view_open_is_special_folder_bootstrap
  - functions/tools/rca_outlook_trace_summary/move_default_view_folder_open_without_rows_to_special_folder_bootstrap
  - functions/tools/rca_outlook_trace_summary/refine_default_view_folder_open_without_rows_key
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/tools/rca_outlook_trace_summary/default_view_folder_open_setprops_key
  - functions/tools/rca_outlook_trace_summary/decrement_default_view_folder_open_without_rows
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_without_rows_classifies_role_and_folder
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_records_followup_default_folder_setprops
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_refines_counter_from_disconnect_trace
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_refines_single_pending_key_from_setprops
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_suppresses_special_folder_bootstrap
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_keeps_noncanonical_setprops_actionable
---

# Signature

`def record_default_view_folder_open_without_rows( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [default_view_open_is_special_folder_bootstrap](../../../functions/tools/rca_outlook_trace_summary/default_view_open_is_special_folder_bootstrap.md)
- [move_default_view_folder_open_without_rows_to_special_folder_bootstrap](../../../functions/tools/rca_outlook_trace_summary/move_default_view_folder_open_without_rows_to_special_folder_bootstrap.md)
- [refine_default_view_folder_open_without_rows_key](../../../functions/tools/rca_outlook_trace_summary/refine_default_view_folder_open_without_rows_key.md)
- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [default_view_folder_open_setprops_key](../../../functions/tools/rca_outlook_trace_summary/default_view_folder_open_setprops_key.md)
- [decrement_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/decrement_default_view_folder_open_without_rows.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_default_view_folder_open_without_rows_classifies_role_and_folder](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_without_rows_classifies_role_and_folder.md)
- [test_default_view_folder_open_records_followup_default_folder_setprops](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_records_followup_default_folder_setprops.md)
- [test_default_view_folder_open_refines_counter_from_disconnect_trace](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_refines_counter_from_disconnect_trace.md)
- [test_default_view_folder_open_refines_single_pending_key_from_setprops](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_refines_single_pending_key_from_setprops.md)
- [test_default_view_folder_open_suppresses_special_folder_bootstrap](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_suppresses_special_folder_bootstrap.md)
- [test_default_view_folder_open_keeps_noncanonical_setprops_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_folder_open_keeps_noncanonical_setprops_actionable.md)