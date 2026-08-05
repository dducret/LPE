---
type: Python Function
title: record_query_rows_terminal_origin_mismatch
resource: tools/rca_outlook_trace_summary.py#L1715-L1741
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_terminal_query_rows_current_origin_is_actionable
---

# Signature

`def record_query_rows_terminal_origin_mismatch( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_terminal_query_rows_current_origin_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_terminal_query_rows_current_origin_is_actionable.md)