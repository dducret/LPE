---
type: Python Function
title: record_visible_release_request_metrics
resource: tools/rca_outlook_trace_summary.py#L1150-L1188
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_visible_release_context
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_records_associated_prefix_find_context
---

# Signature

`def record_visible_release_request_metrics(summary: dict[str, Any], text: str) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [record_visible_release_context](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_context.md)
- [test_visible_release_records_associated_prefix_find_context](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_records_associated_prefix_find_context.md)