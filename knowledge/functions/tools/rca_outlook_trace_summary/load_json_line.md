---
type: Python Function
title: load_json_line
resource: tools/rca_outlook_trace_summary.py#L237-L244
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_rr
  - functions/tools/rca_outlook_trace_summary/summarize_log
---

# Signature

`def load_json_line(line: str) -> dict[str, Any] | None:`

# Called by

- [summarize_rr](../../../functions/tools/rca_outlook_trace_summary/summarize_rr.md)
- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)