---
type: Python Function
title: descriptor_gap_is_actionable
resource: tools/rca_outlook_trace_summary.py#L2171-L2180
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/actionable_descriptor_gap_counts
  - functions/tools/rca_outlook_trace_summary/issue_buckets
---

# Signature

`def descriptor_gap_is_actionable(key: str) -> bool:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [actionable_descriptor_gap_counts](../../../functions/tools/rca_outlook_trace_summary/actionable_descriptor_gap_counts.md)
- [issue_buckets](../../../functions/tools/rca_outlook_trace_summary/issue_buckets.md)