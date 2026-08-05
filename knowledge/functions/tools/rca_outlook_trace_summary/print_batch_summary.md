---
type: Python Function
title: print_batch_summary
resource: tools/rca_outlook_trace_summary.py#L2730-L3720
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/selected_batch_runs
  - functions/tools/rca_outlook_trace_summary/indexed_log_files
  - functions/tools/rca_outlook_trace_summary/matching_log_for_run
  - functions/tools/rca_outlook_trace_summary/summarize_rr
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/rca_outlook_trace_summary/verdict_for_summary
  - functions/tools/rca_outlook_trace_summary/build_scope_for
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/format_build_dirty
  - functions/tools/rca_outlook_trace_summary/issue_buckets
  - functions/tools/rca_outlook_trace_summary/print_counter
  - functions/tools/rca_outlook_trace_summary/unknown_tag_class_counts
  - functions/tools/rca_outlook_trace_summary/actionable_descriptor_gap_counts
  - functions/tools/rca_outlook_trace_summary/print_build_issue_counts
  called_by:
  - functions/tools/rca_outlook_trace_summary/main
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_batch_summary_prints_current_setcolumns_release_handle_classifications
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_batch_summary_warns_when_current_build_has_no_matching_runs
---

# Signature

`def print_batch_summary( trace_root: Path, logs_root: Path, current_build: str | None, last_runs: int | None = None, since_run: str | None = None, ) -> int:`

# Calls

- [selected_batch_runs](../../../functions/tools/rca_outlook_trace_summary/selected_batch_runs.md)
- [indexed_log_files](../../../functions/tools/rca_outlook_trace_summary/indexed_log_files.md)
- [matching_log_for_run](../../../functions/tools/rca_outlook_trace_summary/matching_log_for_run.md)
- [summarize_rr](../../../functions/tools/rca_outlook_trace_summary/summarize_rr.md)
- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [verdict_for_summary](../../../functions/tools/rca_outlook_trace_summary/verdict_for_summary.md)
- [build_scope_for](../../../functions/tools/rca_outlook_trace_summary/build_scope_for.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [format_build_dirty](../../../functions/tools/rca_outlook_trace_summary/format_build_dirty.md)
- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)
- [print_counter](../../../functions/tools/rca_outlook_trace_summary/print_counter.md)
- [unknown_tag_class_counts](../../../functions/tools/rca_outlook_trace_summary/unknown_tag_class_counts.md)
- [actionable_descriptor_gap_counts](../../../functions/tools/rca_outlook_trace_summary/actionable_descriptor_gap_counts.md)
- [print_build_issue_counts](../../../functions/tools/rca_outlook_trace_summary/print_build_issue_counts.md)

# Called by

- [main](../../../functions/tools/rca_outlook_trace_summary/main.md)
- [test_batch_summary_prints_current_setcolumns_release_handle_classifications](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_batch_summary_prints_current_setcolumns_release_handle_classifications.md)
- [test_batch_summary_warns_when_current_build_has_no_matching_runs](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_batch_summary_warns_when_current_build_has_no_matching_runs.md)