---
type: Python Function
title: record_default_view_descriptor_identity_columns
resource: tools/rca_outlook_trace_summary.py#L1830-L1854
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/tools/rca_outlook_trace_summary/suffix_field
  - functions/tools/rca_outlook_trace_summary/csv_field_values
  called_by:
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_descriptor_identity_columns_are_reported_by_role
---

# Signature

`def record_default_view_descriptor_identity_columns( summary: dict[str, Any], text: str ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [suffix_field](../../../functions/tools/rca_outlook_trace_summary/suffix_field.md)
- [csv_field_values](../../../functions/tools/rca_outlook_trace_summary/csv_field_values.md)

# Called by

- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)
- [test_default_view_descriptor_identity_columns_are_reported_by_role](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_descriptor_identity_columns_are_reported_by_role.md)