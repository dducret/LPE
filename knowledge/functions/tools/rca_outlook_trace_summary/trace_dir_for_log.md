---
type: Python Function
title: trace_dir_for_log
resource: tools/rca_outlook_trace_summary.py#L4086-L4112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/trace_jsonl_paths
  - functions/tools/rca_outlook_trace_summary/parse_stamp
  called_by:
  - functions/tools/rca_outlook_trace_summary/main
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_trace_dir_for_log_uses_matching_child_run_directory
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_trace_dir_for_log_uses_nearest_child_run_directory
---

# Signature

`def trace_dir_for_log(trace_dir: Path, log_path: Path | None) -> Path:`

# Calls

- [trace_jsonl_paths](../../../functions/tools/rca_outlook_trace_summary/trace_jsonl_paths.md)
- [parse_stamp](../../../functions/tools/rca_outlook_trace_summary/parse_stamp.md)

# Called by

- [main](../../../functions/tools/rca_outlook_trace_summary/main.md)
- [test_trace_dir_for_log_uses_matching_child_run_directory](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_trace_dir_for_log_uses_matching_child_run_directory.md)
- [test_trace_dir_for_log_uses_nearest_child_run_directory](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_trace_dir_for_log_uses_nearest_child_run_directory.md)