---
type: Python Function
title: matching_log_for_run
resource: tools/rca_outlook_trace_summary.py#L4069-L4083
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/parse_stamp
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
---

# Signature

`def matching_log_for_run(run_name: str, logs_by_stamp: dict[str, Path]) -> Path | None:`

# Calls

- [parse_stamp](../../../functions/tools/rca_outlook_trace_summary/parse_stamp.md)

# Called by

- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)