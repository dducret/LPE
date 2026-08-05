---
type: Python Function
title: move_default_view_folder_open_without_rows_to_special_folder_bootstrap
resource: tools/rca_outlook_trace_summary.py#L1621-L1627
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/decrement_default_view_folder_open_without_rows
  - functions/tools/rca_outlook_trace_summary/default_view_folder_open_setprops_key
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows
---

# Signature

`def move_default_view_folder_open_without_rows_to_special_folder_bootstrap( summary: dict[str, Any], base_key: str, setprops_context: str ) -> None:`

# Calls

- [decrement_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/decrement_default_view_folder_open_without_rows.md)
- [default_view_folder_open_setprops_key](../../../functions/tools/rca_outlook_trace_summary/default_view_folder_open_setprops_key.md)

# Called by

- [record_default_view_folder_open_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_folder_open_without_rows.md)