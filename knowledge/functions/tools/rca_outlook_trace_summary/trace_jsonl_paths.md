---
type: Python Function
title: trace_jsonl_paths
resource: tools/rca_outlook_trace_summary.py#L372-L380
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_rr
  - functions/tools/rca_outlook_trace_summary/trace_dir_for_log
---

# Signature

`def trace_jsonl_paths(trace_dir: Path) -> list[Path]:`

# Called by

- [summarize_rr](../../../functions/tools/rca_outlook_trace_summary/summarize_rr.md)
- [trace_dir_for_log](../../../functions/tools/rca_outlook_trace_summary/trace_dir_for_log.md)