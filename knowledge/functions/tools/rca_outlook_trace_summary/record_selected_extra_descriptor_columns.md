---
type: Python Function
title: record_selected_extra_descriptor_columns
resource: tools/rca_outlook_trace_summary.py#L1800-L1819
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/csv_field_values
  - functions/tools/rca_outlook_trace_summary/nested_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_descriptor_gap
---

# Signature

`def record_selected_extra_descriptor_columns( summary: dict[str, Any], table_kind: str, folder_role: str | None, view_name: str | None, missing: str, text: str, ) -> None:`

# Calls

- [csv_field_values](../../../functions/tools/rca_outlook_trace_summary/csv_field_values.md)
- [nested_field](../../../functions/tools/rca_outlook_trace_summary/nested_field.md)

# Called by

- [record_descriptor_gap](../../../functions/tools/rca_outlook_trace_summary/record_descriptor_gap.md)