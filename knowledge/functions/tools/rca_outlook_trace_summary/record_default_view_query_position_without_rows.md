---
type: Python Function
title: record_default_view_query_position_without_rows
resource: tools/rca_outlook_trace_summary.py#L1472-L1522
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/tools/rca_outlook_trace_summary/int_text_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
  - functions/tools/rca_outlook_trace_summary/record_query_position_wire_fields
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_calendar_query_position_without_rows_flags_zero_duration_timed_row
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_query_position_without_rows_classifies_calendar
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_query_position_without_rows_classifies_generic_role
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_query_position_wire_deduplicates_direct_and_trace_event
---

# Signature

`def record_default_view_query_position_without_rows( summary: dict[str, Any], text: str ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [int_text_field](../../../functions/tools/rca_outlook_trace_summary/int_text_field.md)

# Called by

- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)
- [record_query_position_wire_fields](../../../functions/tools/rca_outlook_trace_summary/record_query_position_wire_fields.md)
- [test_calendar_query_position_without_rows_flags_zero_duration_timed_row](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_calendar_query_position_without_rows_flags_zero_duration_timed_row.md)
- [test_default_view_query_position_without_rows_classifies_calendar](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_query_position_without_rows_classifies_calendar.md)
- [test_default_view_query_position_without_rows_classifies_generic_role](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_query_position_without_rows_classifies_generic_role.md)
- [test_query_position_wire_deduplicates_direct_and_trace_event](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_query_position_wire_deduplicates_direct_and_trace_event.md)