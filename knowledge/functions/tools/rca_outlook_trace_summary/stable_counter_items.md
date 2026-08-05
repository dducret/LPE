---
type: Python Function
title: stable_counter_items
resource: tools/rca_outlook_trace_summary.py#L3947-L3948
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_trace_summary/issue_buckets
  - functions/tools/rca_outlook_trace_summary/problem_getprops_property_type_counts
---

# Signature

`def stable_counter_items(counter: Counter[str], limit: int) -> list[tuple[str, int]]:`

# Called by

- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)
- [problem_getprops_property_type_counts](../../../functions/tools/rca_outlook_trace_summary/problem_getprops_property_type_counts.md)