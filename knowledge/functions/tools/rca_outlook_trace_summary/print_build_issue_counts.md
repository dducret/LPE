---
type: Python Function
title: print_build_issue_counts
resource: tools/rca_outlook_trace_summary.py#L4047-L4057
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_print_build_issue_counts_accepts_custom_title
---

# Signature

`def print_build_issue_counts( counter: Counter[tuple[str, str]], title: str = "Issue buckets by build" ) -> None:`

# Called by

- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [test_print_build_issue_counts_accepts_custom_title](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_print_build_issue_counts_accepts_custom_title.md)