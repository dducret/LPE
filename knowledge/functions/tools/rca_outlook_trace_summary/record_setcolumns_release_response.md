---
type: Python Function
title: record_setcolumns_release_response
resource: tools/rca_outlook_trace_summary.py#L1357-L1370
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/classify_setcolumns_release_response_handle_table
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_setcolumns_release_response_frame_is_counted_from_execute_fields
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_setcolumns_release_response_classifies_invalidated_handle_slot
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_setcolumns_release_response_checks_all_release_slots
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_setcolumns_release_response_classifies_generic_execute_copy_without_raw_frames
---

# Signature

`def record_setcolumns_release_response( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [classify_setcolumns_release_response_handle_table](../../../functions/tools/rca_outlook_trace_summary/classify_setcolumns_release_response_handle_table.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_setcolumns_release_response_frame_is_counted_from_execute_fields](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_setcolumns_release_response_frame_is_counted_from_execute_fields.md)
- [test_setcolumns_release_response_classifies_invalidated_handle_slot](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_setcolumns_release_response_classifies_invalidated_handle_slot.md)
- [test_setcolumns_release_response_checks_all_release_slots](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_setcolumns_release_response_checks_all_release_slots.md)
- [test_setcolumns_release_response_classifies_generic_execute_copy_without_raw_frames](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_setcolumns_release_response_classifies_generic_execute_copy_without_raw_frames.md)