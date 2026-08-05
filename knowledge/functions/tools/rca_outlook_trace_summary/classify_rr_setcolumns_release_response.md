---
type: Python Function
title: classify_rr_setcolumns_release_response
resource: tools/rca_outlook_trace_summary.py#L314-L323
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/parse_rr_response_handle_table
  - functions/tools/rca_outlook_trace_summary/parse_handle_table_summary
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_rr
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_rr_summary_classifies_stale_released_handle
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_rr_summary_classifies_invalidated_released_handle
---

# Signature

`def classify_rr_setcolumns_release_response(metadata: dict[str, Any]) -> str:`

# Calls

- [parse_rr_response_handle_table](../../../functions/tools/rca_outlook_trace_summary/parse_rr_response_handle_table.md)
- [parse_handle_table_summary](../../../functions/tools/rca_outlook_trace_summary/parse_handle_table_summary.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [summarize_rr](../../../functions/tools/rca_outlook_trace_summary/summarize_rr.md)
- [test_rr_summary_classifies_stale_released_handle](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_rr_summary_classifies_stale_released_handle.md)
- [test_rr_summary_classifies_invalidated_released_handle](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_rr_summary_classifies_invalidated_released_handle.md)