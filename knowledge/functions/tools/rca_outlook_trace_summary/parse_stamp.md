---
type: Python Function
title: parse_stamp
resource: tools/rca_outlook_trace_summary.py#L4115-L4122
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/matching_log_for_run
  - functions/tools/rca_outlook_trace_summary/trace_dir_for_log
---

# Signature

`def parse_stamp(value: str) -> datetime | None:`

# Called by

- [matching_log_for_run](../../../functions/tools/rca_outlook_trace_summary/matching_log_for_run.md)
- [trace_dir_for_log](../../../functions/tools/rca_outlook_trace_summary/trace_dir_for_log.md)