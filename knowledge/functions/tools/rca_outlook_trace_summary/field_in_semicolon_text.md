---
type: Python Function
title: field_in_semicolon_text
resource: tools/rca_outlook_trace_summary.py#L1978-L1979
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_visible_inbox_query_rows
  - functions/tools/rca_outlook_trace_summary/record_folder_local_default_view_visibility
---

# Signature

`def field_in_semicolon_text(text: str, key: str) -> str | None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [record_visible_inbox_query_rows](../../../functions/tools/rca_outlook_trace_summary/record_visible_inbox_query_rows.md)
- [record_folder_local_default_view_visibility](../../../functions/tools/rca_outlook_trace_summary/record_folder_local_default_view_visibility.md)