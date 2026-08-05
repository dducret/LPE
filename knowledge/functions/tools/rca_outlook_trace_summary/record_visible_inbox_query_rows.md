---
type: Python Function
title: record_visible_inbox_query_rows
resource: tools/rca_outlook_trace_summary.py#L705-L717
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/field_in_semicolon_text
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_query_rows_event_is_tracked
---

# Signature

`def record_visible_inbox_query_rows( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [field_in_semicolon_text](../../../functions/tools/rca_outlook_trace_summary/field_in_semicolon_text.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_visible_inbox_query_rows_event_is_tracked](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_query_rows_event_is_tracked.md)