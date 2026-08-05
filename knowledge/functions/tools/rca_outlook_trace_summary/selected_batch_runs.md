---
type: Python Function
title: selected_batch_runs
resource: tools/rca_outlook_trace_summary.py#L3951-L3961
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_selected_batch_runs_filters_newest_runs
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_selected_batch_runs_filters_since_run
---

# Signature

`def selected_batch_runs( trace_root: Path, last_runs: int | None = None, since_run: str | None = None ) -> list[Path]:`

# Called by

- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [test_selected_batch_runs_filters_newest_runs](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_selected_batch_runs_filters_newest_runs.md)
- [test_selected_batch_runs_filters_since_run](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_selected_batch_runs_filters_since_run.md)