---
type: Python Function
title: record_descriptor_gap
resource: tools/rca_outlook_trace_summary.py#L1767-L1797
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/tools/rca_outlook_trace_summary/record_selected_extra_descriptor_columns
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_descriptor_gap_classifies_associated_and_visible_tables
---

# Signature

`def record_descriptor_gap( summary: dict[str, Any], text: str, fields: dict[str, Any] | None = None ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [record_selected_extra_descriptor_columns](../../../functions/tools/rca_outlook_trace_summary/record_selected_extra_descriptor_columns.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)
- [test_descriptor_gap_classifies_associated_and_visible_tables](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_descriptor_gap_classifies_associated_and_visible_tables.md)