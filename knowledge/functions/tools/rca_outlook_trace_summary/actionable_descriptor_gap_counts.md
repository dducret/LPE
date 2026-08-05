---
type: Python Function
title: actionable_descriptor_gap_counts
resource: tools/rca_outlook_trace_summary.py#L2233-L2236
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/descriptor_gap_is_actionable
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_single_summary
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_actionable_descriptor_gap_counts_filters_backed_columns
---

# Signature

`def actionable_descriptor_gap_counts(counter: Counter[str]) -> Counter[str]:`

# Calls

- [descriptor_gap_is_actionable](../../../functions/tools/rca_outlook_trace_summary/descriptor_gap_is_actionable.md)

# Called by

- [print_single_summary](../../../functions/tools/rca_outlook_trace_summary/print_single_summary.md)
- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [test_actionable_descriptor_gap_counts_filters_backed_columns](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_actionable_descriptor_gap_counts_filters_backed_columns.md)