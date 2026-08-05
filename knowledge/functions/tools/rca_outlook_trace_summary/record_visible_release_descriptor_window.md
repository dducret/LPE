---
type: Python Function
title: record_visible_release_descriptor_window
resource: tools/rca_outlook_trace_summary.py#L1857-L1876
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/tools/rca_outlook_trace_summary/suffix_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_visible_release_context
---

# Signature

`def record_visible_release_descriptor_window( summary: dict[str, Any], segment: str ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [suffix_field](../../../functions/tools/rca_outlook_trace_summary/suffix_field.md)

# Called by

- [record_visible_release_context](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_context.md)