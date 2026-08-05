---
type: Python Function
title: record_query_position_wire_fields
resource: tools/rca_outlook_trace_summary.py#L1525-L1535
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/record_default_view_query_position_without_rows
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_query_position_wire_fields_are_classified_directly
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_query_position_wire_deduplicates_direct_and_trace_event
---

# Signature

`def record_query_position_wire_fields( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_default_view_query_position_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_query_position_without_rows.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_query_position_wire_fields_are_classified_directly](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_query_position_wire_fields_are_classified_directly.md)
- [test_query_position_wire_deduplicates_direct_and_trace_event](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_query_position_wire_deduplicates_direct_and_trace_event.md)