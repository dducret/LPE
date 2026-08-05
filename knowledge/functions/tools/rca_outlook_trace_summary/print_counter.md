---
type: Python Function
title: print_counter
resource: tools/rca_outlook_trace_summary.py#L2224-L2230
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_single_summary
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
---

# Signature

`def print_counter(title: str, counter: Counter[str], limit: int = 12) -> None:`

# Called by

- [print_single_summary](../../../functions/tools/rca_outlook_trace_summary/print_single_summary.md)
- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)