---
type: Python Function
title: actionable_zero_default_tag_counts
resource: tools/rca_outlook_trace_summary.py#L4037-L4044
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_single_summary
  - functions/tools/rca_outlook_trace_summary/issue_buckets
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_flags_empty_structured_folder_view_streams
---

# Signature

`def actionable_zero_default_tag_counts(counter: Counter[str]) -> Counter[str]:`

# Called by

- [print_single_summary](../../../functions/tools/rca_outlook_trace_summary/print_single_summary.md)
- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)
- [test_issue_buckets_flags_empty_structured_folder_view_streams](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_issue_buckets_flags_empty_structured_folder_view_streams.md)