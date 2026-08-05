---
type: Python Function
title: record_visible_release_descriptor_contract
resource: tools/rca_outlook_trace_summary.py#L1906-L1945
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/tools/rca_outlook_trace_summary/suffix_field
  - functions/tools/rca_outlook_trace_summary/csv_field_values
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_visible_release_context
---

# Signature

`def record_visible_release_descriptor_contract( summary: dict[str, Any], segment: str ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [suffix_field](../../../functions/tools/rca_outlook_trace_summary/suffix_field.md)
- [csv_field_values](../../../functions/tools/rca_outlook_trace_summary/csv_field_values.md)

# Called by

- [record_visible_release_context](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_context.md)