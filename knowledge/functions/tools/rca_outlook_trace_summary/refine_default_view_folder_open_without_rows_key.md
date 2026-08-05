---
type: Python Function
title: refine_default_view_folder_open_without_rows_key
resource: tools/rca_outlook_trace_summary.py#L1613-L1618
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/default_view_folder_open_setprops_key
  - functions/tools/rca_outlook_trace_summary/decrement_default_view_folder_open_without_rows
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows
---

# Signature

`def refine_default_view_folder_open_without_rows_key( summary: dict[str, Any], base_key: str, setprops_context: str ) -> None:`

# Calls

- [default_view_folder_open_setprops_key](../../../functions/tools/rca_outlook_trace_summary/default_view_folder_open_setprops_key.md)
- [decrement_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/decrement_default_view_folder_open_without_rows.md)

# Called by

- [record_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows.md)