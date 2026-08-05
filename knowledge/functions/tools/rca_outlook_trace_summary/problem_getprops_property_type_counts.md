---
type: Python Function
title: problem_getprops_property_type_counts
resource: tools/rca_outlook_trace_summary.py#L3964-L3972
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/stable_counter_items
  called_by:
  - functions/tools/rca_outlook_trace_summary/issue_buckets
---

# Signature

`def problem_getprops_property_type_counts(counter: Counter[str]) -> list[tuple[str, int]]:`

# Calls

- [stable_counter_items](../../../functions/tools/rca_outlook_trace_summary/stable_counter_items.md)

# Called by

- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)